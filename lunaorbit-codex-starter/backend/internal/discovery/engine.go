package discovery

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"lunaorbit/backend/internal/discovery/parser"
	"lunaorbit/backend/internal/discovery/resource"
	"regexp"
	"sort"
	"strings"
)

var safeName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,252}$`)
var safeCode = regexp.MustCompile(`^[A-Z_]{1,64}$`)

func identity(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:16])
}
func addNote(notes []string, value string) []string {
	for _, n := range notes {
		if n == value {
			return notes
		}
	}
	return append(notes, value)
}
func supportedFile(key string) bool {
	lower := strings.ToLower(key)
	return safeName.MatchString(key) && (strings.HasSuffix(lower, ".properties") || strings.HasSuffix(lower, ".yaml") || strings.HasSuffix(lower, ".yml"))
}
func metadataOK(meta resource.Metadata, namespace string) bool {
	return meta.Namespace == namespace && safeName.MatchString(meta.Name) && safeName.MatchString(meta.UID) && len(meta.ResourceVersion) <= 256
}

type reference struct{ name, key, kind string }

func references(dep resource.Deployment) []reference {
	var result []reference
	seen := map[string]bool{}
	add := func(name, key, kind string) {
		if !safeName.MatchString(name) || (key != "" && !safeName.MatchString(key)) {
			return
		}
		id := name + "\x00" + key + "\x00" + kind
		if !seen[id] {
			seen[id] = true
			result = append(result, reference{name, key, kind})
		}
	}
	for _, volume := range dep.Spec.Template.Spec.Volumes {
		if volume.ConfigMap != nil {
			add(volume.ConfigMap.Name, "", "volume.configMap")
		}
	}
	containers := append(append([]resource.Container{}, dep.Spec.Template.Spec.Containers...), dep.Spec.Template.Spec.InitContainers...)
	for _, container := range containers {
		for _, env := range container.Env {
			if env.ValueFrom.ConfigMapKeyRef != nil {
				ref := env.ValueFrom.ConfigMapKeyRef
				add(ref.Name, ref.Key, "env.configMapKeyRef")
			}
		}
		for _, env := range container.EnvFrom {
			if env.ConfigMapRef != nil {
				add(env.ConfigMapRef.Name, "", "envFrom.configMapRef")
			}
		}
	}
	return result
}
func mm2Hint(dep resource.Deployment) bool {
	containers := append(append([]resource.Container{}, dep.Spec.Template.Spec.Containers...), dep.Spec.Template.Spec.InitContainers...)
	for _, container := range containers {
		for _, value := range append(append([]string{container.Image}, container.Command...), container.Args...) {
			text := strings.ToLower(value)
			if strings.Contains(text, "mm2") || strings.Contains(text, "mirrormaker") || strings.Contains(text, "mirror-maker") || strings.Contains(text, "org.apache.kafka.connect.mirror") {
				return true
			}
		}
	}
	return false
}
func runtimeProjection(dep resource.Deployment, pods []resource.Pod) Runtime {
	result := Runtime{Generation: dep.Metadata.Generation, ObservedGeneration: dep.Status.ObservedGeneration, Replicas: dep.Status.Replicas, ReadyReplicas: dep.Status.ReadyReplicas, PodAssociation: "unknown"}
	selector := dep.Spec.Selector.MatchLabels
	if len(selector) > 0 {
		result.PodAssociation = "label_candidates_only"
		for _, pod := range pods {
			if !metadataOK(pod.Metadata, dep.Metadata.Namespace) {
				continue
			}
			matches := true
			for key, value := range selector {
				if pod.Metadata.Labels[key] != value {
					matches = false
					break
				}
			}
			if matches {
				result.PodCandidates++
			}
		}
	}
	return result
}

func Build(col resource.Collection, registrations []Registration, bindings []ManualBinding, identities []AliasIdentity, previous Snapshot) (Snapshot, error) {
	if !safeName.MatchString(col.Scope.ID) || col.ObservedAt.IsZero() || len(col.Namespaces) > 32 {
		return Snapshot{}, errors.New("invalid discovery collection")
	}
	if previous.ScopeID != "" && previous.ScopeID != col.Scope.ID {
		return Snapshot{}, errors.New("discovery scope mismatch")
	}
	result := Snapshot{ScopeID: col.Scope.ID, Synthetic: col.Scope.Synthetic, ObservedAt: col.ObservedAt, AttemptedAt: col.ObservedAt, Links: []Link{}, Nodes: []Node{}, Edges: []Edge{}, Issues: []Issue{}}
	current := map[string]Link{}
	complete := col.ErrorCode == ""
	totalBytes := 0
	issue := func(code, ns, uid, cm string) {
		if len(result.Issues) >= 256 {
			return
		}
		code = strings.ToUpper(code)
		if !safeCode.MatchString(code) {
			code = "COLLECTION_FAILED"
		}
		result.Issues = append(result.Issues, Issue{Code: code, Namespace: ns, WorkloadUID: uid, ConfigUID: cm})
	}
	if col.ErrorCode != "" {
		issue(col.ErrorCode, "", "", "")
	}
	seenNamespaces := map[string]bool{}
	failedNamespaces := map[string]bool{}
	evidenceCount := 0
	if col.ErrorCode == "" {
		for _, batch := range col.Namespaces {
			if !safeName.MatchString(batch.Namespace) || seenNamespaces[batch.Namespace] {
				return Snapshot{}, errors.New("invalid namespace collection")
			}
			seenNamespaces[batch.Namespace] = true
			if !batch.Complete {
				complete = false
				failedNamespaces[batch.Namespace] = true
				issue(batch.ErrorCode, batch.Namespace, "", "")
				continue
			}
			if len(batch.Deployments) > 256 || len(batch.ConfigMaps) > 512 || len(batch.Pods) > 1024 {
				return Snapshot{}, errors.New("discovery resource limit exceeded")
			}
			configs := map[string][]resource.ConfigMap{}
			for _, cm := range batch.ConfigMaps {
				if metadataOK(cm.Metadata, batch.Namespace) {
					configs[cm.Metadata.Name] = append(configs[cm.Metadata.Name], cm)
				}
			}
			seenWorkloads := map[string]bool{}
			for _, dep := range batch.Deployments {
				if !metadataOK(dep.Metadata, batch.Namespace) {
					issue("INVALID_RESOURCE_METADATA", batch.Namespace, "", "")
					continue
				}
				if seenWorkloads[dep.Metadata.UID] {
					issue("DUPLICATE_WORKLOAD", batch.Namespace, dep.Metadata.UID, "")
					continue
				}
				seenWorkloads[dep.Metadata.UID] = true
				for _, ref := range references(dep) {
					matches := configs[ref.name]
					if len(matches) != 1 {
						code := "CONFIGMAP_MISSING"
						if len(matches) > 1 {
							code = "CONFIGMAP_AMBIGUOUS"
						}
						issue(code, batch.Namespace, dep.Metadata.UID, "")
						continue
					}
					cm := matches[0]
					if len(cm.Data) > 64 {
						issue("CONFIGURATION_LIMIT", batch.Namespace, dep.Metadata.UID, cm.Metadata.UID)
						continue
					}
					keys := make([]string, 0, len(cm.Data))
					for key := range cm.Data {
						if supportedFile(key) && (ref.key == "" || ref.key == key) {
							keys = append(keys, key)
						}
					}
					sort.Strings(keys)
					if ref.key != "" && len(keys) == 0 {
						issue("CONFIG_KEY_MISSING", batch.Namespace, dep.Metadata.UID, cm.Metadata.UID)
					}
					for _, key := range keys {
						totalBytes += len(cm.Data[key])
						if totalBytes > 8<<20 {
							return Snapshot{}, errors.New("discovery configuration budget exceeded")
						}
						config, err := parser.Parse(key, cm.Data[key])
						if err != nil {
							issue("CONFIGURATION_INVALID", batch.Namespace, dep.Metadata.UID, cm.Metadata.UID)
							continue
						}
						for _, code := range config.Issues {
							issue(code, batch.Namespace, dep.Metadata.UID, cm.Metadata.UID)
						}
						for _, rule := range config.Rules {
							if !safeName.MatchString(rule.Source) || !safeName.MatchString(rule.Target) || rule.Source == rule.Target {
								issue("INVALID_DIRECTION", batch.Namespace, dep.Metadata.UID, cm.Metadata.UID)
								continue
							}
							aliases := map[string]bool{}
							for _, alias := range config.DeclaredAliases {
								aliases[alias] = true
							}
							known := aliases[rule.Source] && aliases[rule.Target]
							if len(config.DeclaredAliases) < 2 && !mm2Hint(dep) {
								issue("INSUFFICIENT_MM2_EVIDENCE", batch.Namespace, dep.Metadata.UID, cm.Metadata.UID)
								continue
							}
							id := identity(col.Scope.ID, batch.Namespace, dep.Metadata.UID, rule.Source, rule.Target)
							link, exists := current[id]
							declaration := Declaration{InternalSeparatorEnabled: rule.InternalSeparatorEnabled, TopicsExcludeRaw: rule.TopicsExcludeRaw, Separator: rule.Separator, TopicFilterClass: rule.TopicFilterClass, SemanticsKnown: rule.SemanticsKnown, TopicsRaw: rule.TopicsRaw, Policy: rule.Policy, Enabled: rule.Enabled, EnabledExplicit: rule.EnabledExplicit, SourceBootstrapFingerprint: bootstrapFingerprint(config.Bootstraps[rule.Source]), TargetBootstrapFingerprint: bootstrapFingerprint(config.Bootstraps[rule.Target]), ConfigMapUID: cm.Metadata.UID, ConfigKey: key}
							sourceMapping := Match(col.Scope.ID, batch.Namespace, dep.Metadata.UID, rule.Source, config.Bootstraps[rule.Source], registrations, bindings, identities)
							targetMapping := Match(col.Scope.ID, batch.Namespace, dep.Metadata.UID, rule.Target, config.Bootstraps[rule.Target], registrations, bindings, identities)
							if !exists {
								if len(current) >= 1024 {
									return Snapshot{}, errors.New("discovery link budget exceeded")
								}
								link = Link{ID: id, Namespace: batch.Namespace, WorkloadUID: dep.Metadata.UID, WorkloadName: dep.Metadata.Name, SourceAlias: rule.Source, TargetAlias: rule.Target, InternalSeparatorEnabled: rule.InternalSeparatorEnabled, TopicsExcludeRaw: rule.TopicsExcludeRaw, Separator: rule.Separator, TopicFilterClass: rule.TopicFilterClass, SemanticsKnown: rule.SemanticsKnown, TopicsRaw: rule.TopicsRaw, Policy: rule.Policy, Enabled: rule.Enabled, Confidence: "medium", Declared: known && rule.EnabledExplicit, Observed: true, Verified: false, State: "declared", ObservedAt: col.ObservedAt, Runtime: runtimeProjection(dep, batch.Pods), Evidence: []Evidence{}, Notes: []string{}, Declarations: []Declaration{}}
								if !known || !rule.EnabledExplicit {
									link.State = "unknown"
									link.Confidence = "low"
									link.Notes = addNote(link.Notes, "insufficient_configuration_evidence")
								}
								if rule.EnabledExplicit && !rule.Enabled {
									link.State = "disabled"
								}
								link.Source = sourceMapping
								link.Target = targetMapping
							} else {
								first := link.Declarations[0]
								conflict := first.InternalSeparatorEnabled != declaration.InternalSeparatorEnabled || first.TopicsExcludeRaw != declaration.TopicsExcludeRaw || first.Separator != declaration.Separator || first.TopicFilterClass != declaration.TopicFilterClass || first.SemanticsKnown != declaration.SemanticsKnown || first.TopicsRaw != declaration.TopicsRaw || first.Policy != declaration.Policy || first.Enabled != declaration.Enabled || first.EnabledExplicit != declaration.EnabledExplicit || first.SourceBootstrapFingerprint != declaration.SourceBootstrapFingerprint || first.TargetBootstrapFingerprint != declaration.TargetBootstrapFingerprint
								link.Declared = link.Declared && known && rule.EnabledExplicit
								if conflict {
									link.State = "unknown"
									link.Confidence = "low"
									link.SemanticsKnown = false
									link.TopicsRaw = ""
									link.Policy = ""
									link.Enabled = false
									if first.SourceBootstrapFingerprint != declaration.SourceBootstrapFingerprint {
										link.Source = conflictingMapping(link.Source, sourceMapping)
									}
									if first.TargetBootstrapFingerprint != declaration.TargetBootstrapFingerprint {
										link.Target = conflictingMapping(link.Target, targetMapping)
									}
									link.Notes = addNote(link.Notes, "configuration_conflict")
									issue("CONFIGURATION_CONFLICT", batch.Namespace, dep.Metadata.UID, cm.Metadata.UID)
								}
							}
							if link.Source.Status != "confirmed" || link.Target.Status != "confirmed" {
								link.Confidence = "low"
							}
							proof := Evidence{ScopeID: col.Scope.ID, Namespace: batch.Namespace, DeploymentUID: dep.Metadata.UID, DeploymentName: dep.Metadata.Name, ConfigMapName: cm.Metadata.Name, ConfigMapUID: cm.Metadata.UID, ResourceVersion: cm.Metadata.ResourceVersion, ConfigKey: key, ReferenceKind: ref.kind, Fingerprint: config.Fingerprint, ObservedAt: col.ObservedAt}
							duplicate := false
							for _, e := range link.Evidence {
								if e.ConfigMapUID == proof.ConfigMapUID && e.ConfigKey == proof.ConfigKey && e.ReferenceKind == proof.ReferenceKind {
									duplicate = true
								}
							}
							if !duplicate {
								if len(link.Evidence) >= 32 || evidenceCount >= 2048 {
									return Snapshot{}, errors.New("discovery evidence budget exceeded")
								}
								evidenceCount++
								link.Evidence = append(link.Evidence, proof)
							}
							found := false
							for _, d := range link.Declarations {
								if d == declaration {
									found = true
								}
							}
							if !found {
								link.Declarations = append(link.Declarations, declaration)
							}
							if (rule.Source == "mars" || rule.Source == "charon") && rule.Target == "moon-prod" {
								link.Notes = addNote(link.Notes, "approved_multi_source: mars+charon -> moon-prod")
							}
							if rule.Source == "styx" && rule.Target == "saturn-uat" {
								link.Notes = addNote(link.Notes, "approved_target: styx -> saturn-uat; historical_name_not_authoritative")
							}
							if rule.Source == "styx" && rule.Target != "saturn-uat" {
								link.State = "unknown"
								link.Notes = addNote(link.Notes, "business_target_conflict: confirmed target saturn-uat")
							}
							current[id] = link
						}
					}
				}
			}
		}
	}
	for _, old := range previous.Links {
		if _, seen := current[old.ID]; !seen {
			old.Stale = true
			old.State = "unknown"
			old.Verified = false
			note := "not_observed_in_complete_collection"
			if col.ErrorCode != "" || failedNamespaces[old.Namespace] {
				note = "collection_failed_evidence_retained"
				// Unknown whether it still exists: keep the evidence and mark the snapshot stale.
				result.Stale = true
			} else {
				// A complete collection did not see it: history only, never current or stale.
				old.Removed = true
			}
			old.Notes = addNote(old.Notes, note)
			current[old.ID] = old
		}
	}
	if len(current) > 1024 {
		return Snapshot{}, errors.New("discovery retained link budget exceeded")
	}
	if !complete {
		result.Stale = true
		result.ObservedAt = previous.ObservedAt
	}
	for _, link := range current {
		result.Links = append(result.Links, link)
	}
	sort.Slice(result.Links, func(i, j int) bool { return result.Links[i].ID < result.Links[j].ID })
	if col.Scope.Auto {
		classifyAuto(&result, col)
	}
	buildGraph(&result)
	return result, nil
}

func classifyAuto(result *Snapshot, col resource.Collection) {
	cluster := col.Scope.Cluster
	linked := map[string]bool{}
	for i := range result.Links {
		result.Links[i].KubeCluster = cluster
		if !result.Links[i].Stale {
			linked[result.Links[i].WorkloadUID] = true
		}
	}
	scan := ScanSummary{KubeCluster: cluster, Namespaces: col.NamespacesScanned, Deployments: col.DeploymentsScanned}
	withMM2 := map[string]bool{}
	for _, batch := range col.Namespaces {
		for _, d := range batch.Deployments {
			signals := batch.Signals[d.Metadata.UID]
			if len(signals) == 0 {
				continue
			}
			status := "suspected"
			if linked[d.Metadata.UID] {
				status = "confirmed"
				scan.Confirmed++
				withMM2[batch.Namespace] = true
			} else {
				scan.Suspected++
			}
			result.Workloads = append(result.Workloads, WorkloadClass{KubeCluster: cluster, Namespace: batch.Namespace, Name: d.Metadata.Name, UID: d.Metadata.UID, Status: status, Signals: append([]string(nil), signals...)})
		}
	}
	scan.NamespacesWithMM2 = len(withMM2)
	sort.Slice(result.Workloads, func(i, j int) bool {
		a, b := result.Workloads[i], result.Workloads[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	result.Scan = &scan
}

func buildGraph(snapshot *Snapshot) {
	nodes := map[string]Node{}
	for _, link := range snapshot.Links {
		if link.Removed {
			continue
		}
		workload := "workload:" + identity(snapshot.ScopeID, link.Namespace, link.WorkloadUID)
		nodes[workload] = Node{ID: workload, Kind: "workload", Label: link.WorkloadName, RFStatus: "not_applicable"}
		endpoint := func(mapping Mapping) string {
			id := "alias:" + identity(snapshot.ScopeID, link.Namespace, link.WorkloadUID, mapping.Alias)
			if mapping.Status == "confirmed" && mapping.RegistrationID != "" {
				id = "kafka:" + mapping.RegistrationID
			}
			nodes[id] = Node{ID: id, Kind: "kafka", Label: mapping.Alias, RegistrationID: mapping.RegistrationID, RFStatus: "unknown"}
			return id
		}
		source, target := endpoint(link.Source), endpoint(link.Target)
		snapshot.Edges = append(snapshot.Edges, Edge{ID: identity(link.ID, "input"), Source: source, Target: workload, LinkID: link.ID, Relation: "declared_input", Evidence: link.Evidence, Stale: link.Stale}, Edge{ID: identity(link.ID, "output"), Source: workload, Target: target, LinkID: link.ID, Relation: "declared_output", Evidence: link.Evidence, Stale: link.Stale})
	}
	for _, node := range nodes {
		snapshot.Nodes = append(snapshot.Nodes, node)
	}
	sort.Slice(snapshot.Nodes, func(i, j int) bool { return snapshot.Nodes[i].ID < snapshot.Nodes[j].ID })
}
