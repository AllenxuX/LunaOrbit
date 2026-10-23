package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"lunaorbit/backend/internal/changes"
	"lunaorbit/backend/internal/discovery"
	"lunaorbit/backend/internal/registry"
)

func configureChanges(store changes.Store, source *discovery.Service, persistent bool, getenv func(string) string) (*changes.Service, error) {
	mode := getenv("CHANGES_MODE")
	if mode == "" || mode == "disabled" {
		return nil, nil
	}
	if (mode != "mock" && mode != "lab") || source == nil {
		return nil, errors.New("invalid changes configuration")
	}
	key, err := hex.DecodeString(getenv("CHANGE_SNAPSHOT_KEY"))
	if err != nil {
		return nil, errors.New("invalid change snapshot key")
	}
	if len(key) == 0 && !persistent {
		key = make([]byte, 32)
		if _, err = rand.Read(key); err != nil {
			return nil, errors.New("change snapshot key unavailable")
		}
	}
	vault, err := changes.NewVault(key)
	if err != nil {
		return nil, errors.New("change snapshot key required")
	}
	catalog := changes.DefaultCatalog()
	var adapter changes.Adapter = changes.NewMockAdapter(catalog)
	if mode == "lab" {
		if getenv("KITE_LAB_VERSION") != "v0.16.0" || getenv("LUNAORBIT_RUNTIME_MODE") != "admin-lab" {
			return nil, errors.New("explicit admin lab mode required")
		}
		raw := getenv("CHANGE_LAB_CATALOG")
		var rows []struct{ ID, Name, ScopeID, Namespace, ConfigMapName, DeploymentName, ConfigDataKey, TopicRuleKey, ConfigMapUID, DeploymentUID string }
		if len(raw) == 0 || len(raw) > 65536 || json.Unmarshal([]byte(raw), &rows) != nil || len(rows) == 0 || len(rows) > 32 {
			return nil, errors.New("invalid lab change catalog")
		}
		catalog = nil
		for _, row := range rows {
			if row.Namespace != getenv("KITE_NAMESPACE") || row.ScopeID != "readonly-lab" || row.ConfigMapName == "" || row.DeploymentName == "" || row.ConfigDataKey == "" || row.TopicRuleKey == "" || row.ConfigMapUID == "" || row.DeploymentUID == "" || row.ID == "" {
				return nil, errors.New("invalid lab change target")
			}
			catalog = append(catalog, changes.Target{ID: row.ID, Name: row.Name, ScopeID: row.ScopeID, Namespace: row.Namespace, WorkloadUID: row.DeploymentUID, ConfigMapUID: row.ConfigMapUID, DeploymentUID: row.DeploymentUID, ConfigMapName: row.ConfigMapName, DeploymentName: row.DeploymentName, ConfigDataKey: row.ConfigDataKey, TopicRuleKey: row.TopicRuleKey})
		}
		adapter, err = changes.NewLabAdapter(getenv("KITE_BASE_URL"), getenv("KITE_WRITE_API_KEY"), getenv("KITE_CLUSTER_NAME"), getenv("ENABLE_KITE_WRITES") == "true")
		if err != nil {
			return nil, errors.New("invalid lab change transport")
		}
	}
	service := changes.NewService(store, vault, catalog, adapter)
	service.EnableWrites = mode == "lab" && getenv("ENABLE_KITE_WRITES") == "true"
	service.LabSelfApproval = mode == "lab" && getenv("CHANGE_LAB_ALLOW_SELF_APPROVAL") == "true"
	var approvals map[string][]string
	if raw := getenv("CHANGE_APPROVAL_SCOPES"); raw != "" {
		if len(raw) > 65536 || json.Unmarshal([]byte(raw), &approvals) != nil {
			return nil, errors.New("invalid change approval scopes")
		}
	}
	service.ApprovalAuthorizer = changes.StaticApprovals(approvals)
	service.SourceAuthorizer = func(ctx context.Context, a registry.Actor, t changes.Target) error {
		s, err := source.Snapshot(ctx, discovery.Actor{ID: a.ID, Role: a.Role}, t.ScopeID)
		if err != nil {
			return changes.ErrNotFound
		}
		for _, l := range s.Links {
			if !l.Stale && l.Namespace == t.Namespace && l.WorkloadUID == t.WorkloadUID && l.WorkloadUID == t.DeploymentUID {
				for _, e := range l.Evidence {
					if e.ScopeID == t.ScopeID && e.Namespace == t.Namespace && e.DeploymentUID == t.DeploymentUID && e.ConfigMapUID == t.ConfigMapUID {
						return nil
					}
				}
			}
		}
		return changes.ErrNotFound
	}
	return service, nil
}
