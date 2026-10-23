package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

type PostgresStore struct{ db *sql.DB }

func NewPostgresStore(db *sql.DB) *PostgresStore { return &PostgresStore{db: db} }
func storageError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrStorage
}
func (s *PostgresStore) Get(ctx context.Context, owner, id string) (Registration, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload FROM kafka_cluster_registrations WHERE id=$1 AND created_by=$2 AND deleted_at IS NULL`, id, owner).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return Registration{}, ErrNotFound
	}
	if err != nil {
		return Registration{}, storageError(err)
	}
	var r Registration
	if json.Unmarshal(raw, &r) != nil {
		return Registration{}, ErrStorage
	}
	return r, nil
}
func (s *PostgresStore) List(ctx context.Context, owner string) ([]Registration, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT payload FROM kafka_cluster_registrations WHERE created_by=$1 AND deleted_at IS NULL ORDER BY id`, owner)
	if err != nil {
		return nil, storageError(err)
	}
	defer rows.Close()
	out := []Registration{}
	for rows.Next() {
		var raw []byte
		var r Registration
		if rows.Scan(&raw) != nil || json.Unmarshal(raw, &r) != nil {
			return nil, ErrStorage
		}
		out = append(out, r)
	}
	if err = rows.Err(); err != nil {
		return nil, storageError(err)
	}
	return out, nil
}
func (s *PostgresStore) Save(ctx context.Context, r Registration, expected int64, action string) error {
	if expected < 0 || r.Revision != expected+1 || r.ID == "" || r.CreatedBy == "" {
		return ErrConflict
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return ErrStorage
	}
	brokers := r.Brokers
	if brokers == nil {
		brokers = []Broker{}
	}
	brokerJSON, err := json.Marshal(brokers)
	if err != nil {
		return ErrStorage
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storageError(err)
	}
	defer tx.Rollback()
	if expected == 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO kafka_cluster_registrations(id,created_by,revision,deleted_at,payload) VALUES($1,$2,$3,$4,$5::jsonb)`, r.ID, r.CreatedBy, r.Revision, r.DeletedAt, string(raw))
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrConflict
		}
	} else {
		var result sql.Result
		result, err = tx.ExecContext(ctx, `UPDATE kafka_cluster_registrations SET revision=$1,deleted_at=$2,payload=$3::jsonb WHERE id=$4 AND created_by=$5 AND revision=$6 AND deleted_at IS NULL`, r.Revision, r.DeletedAt, string(raw), r.ID, r.CreatedBy, expected)
		if err == nil {
			var count int64
			count, err = result.RowsAffected()
			if err == nil && count != 1 {
				return ErrConflict
			}
		}
	}
	if err != nil {
		return storageError(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO kafka_cluster_credentials(cluster_registration_id,security_protocol,sasl_mechanism,username_masked,username_ref,password_ref,tls_ca_ref,tls_cert_ref,tls_key_ref,tls_server_name,revision) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(cluster_registration_id) DO UPDATE SET security_protocol=EXCLUDED.security_protocol,sasl_mechanism=EXCLUDED.sasl_mechanism,username_masked=EXCLUDED.username_masked,username_ref=EXCLUDED.username_ref,password_ref=EXCLUDED.password_ref,tls_ca_ref=EXCLUDED.tls_ca_ref,tls_cert_ref=EXCLUDED.tls_cert_ref,tls_key_ref=EXCLUDED.tls_key_ref,tls_server_name=EXCLUDED.tls_server_name,revision=EXCLUDED.revision`, r.ID, r.SecurityProtocol, r.SASLMechanism, r.UsernameMasked, r.UsernameRef, r.PasswordRef, r.TLSCARef, r.TLSCertRef, r.TLSKeyRef, r.TLSServerName, r.CredentialRevision)
	if err != nil {
		return storageError(err)
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM kafka_cluster_capabilities WHERE cluster_registration_id=$1`, r.ID); err != nil {
		return storageError(err)
	}
	for _, c := range r.Capabilities {
		if _, err = tx.ExecContext(ctx, `INSERT INTO kafka_cluster_capabilities(cluster_registration_id,feature,supported,reason) VALUES($1,$2,$3,$4)`, r.ID, c.Feature, c.Supported, c.Reason); err != nil {
			return storageError(err)
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO kafka_cluster_probe_status(cluster_registration_id,status,last_probe_at,last_probe_error_code,discovered_cluster_id,brokers) VALUES($1,$2,$3,$4,$5,$6::jsonb) ON CONFLICT(cluster_registration_id) DO UPDATE SET status=EXCLUDED.status,last_probe_at=EXCLUDED.last_probe_at,last_probe_error_code=EXCLUDED.last_probe_error_code,discovered_cluster_id=EXCLUDED.discovered_cluster_id,brokers=EXCLUDED.brokers`, r.ID, r.Status, r.LastProbeAt, r.LastProbeErrorCode, r.DiscoveredClusterID, string(brokerJSON))
	if err != nil {
		return storageError(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO audit_events(actor_id,action,target_id,outcome) VALUES($1,$2,$3,'success')`, r.CreatedBy, action, r.ID); err != nil {
		return storageError(err)
	}
	return storageError(tx.Commit())
}
func (s *PostgresStore) RecordAudit(ctx context.Context, actor, action, target, outcome string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO audit_events(actor_id,action,target_id,outcome) VALUES($1,$2,$3,$4)`, actor, action, target, outcome)
	return storageError(err)
}
