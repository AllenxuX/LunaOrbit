// lab-account-import copies previously generated lab passwords only after live
// authentication. It never creates SCRAM credentials or changes Kafka ACLs.
package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"lunaorbit/backend/internal/registry"
	"lunaorbit/backend/internal/workspace"
)

var invalid = errors.New("INVALID_LAB_INPUT")

type input struct {
	DatabaseURL      string `json:"database_url"`
	AccountMasterKey string `json:"account_master_key"`
	RegistrationID   string `json:"registration_id"`
}
type counts struct {
	Existing int `json:"existing"`
	Imported int `json:"imported"`
	Missing  int `json:"missing"`
	Failed   int `json:"failed"`
}

func readInput(r io.Reader, lab bool) (input, []byte, error) {
	var in input
	if !lab {
		return in, nil, invalid
	}
	raw, e := io.ReadAll(io.LimitReader(r, 16385))
	if e != nil || len(raw) > 16384 {
		return in, nil, invalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&in) != nil {
		return input{}, nil, invalid
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return input{}, nil, invalid
	}
	u, e := url.Parse(in.DatabaseURL)
	if e != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Host != "127.0.0.1:5432" || u.Path != "/lunaorbit_phase02_test" || u.Fragment != "" || u.Opaque != "" {
		return input{}, nil, invalid
	}
	q, e := url.ParseQuery(u.RawQuery)
	if e != nil {
		return input{}, nil, invalid
	}
	for k, v := range q {
		if k != "sslmode" || len(v) != 1 {
			return input{}, nil, invalid
		}
	}
	key, e := hex.DecodeString(in.AccountMasterKey)
	if e != nil || len(key) != 32 || bytes.Equal(key, make([]byte, 32)) {
		return input{}, nil, invalid
	}
	id, e := hex.DecodeString(in.RegistrationID)
	if e != nil || len(id) != 16 {
		return input{}, nil, invalid
	}
	return in, key, nil
}
func allowed(r registry.Registration) bool {
	if r.DeletedAt != nil || !r.Enabled || r.Environment != "实验环境" || r.SecurityProtocol != "PLAINTEXT" || len(r.BootstrapServers) != 1 {
		return false
	}
	b := r.BootstrapServers[0]
	return b == "kafka-target.lunaorbit-integration.svc.cluster.local:9092" || b == "kafka-source.lunaorbit-integration.svc.cluster.local:9092"
}
func crypt(key []byte, registration string, a workspace.Account, value string, open bool) (string, error) {
	b, e := aes.NewCipher(key)
	if e != nil {
		return "", invalid
	}
	g, e := cipher.NewGCM(b)
	if e != nil {
		return "", invalid
	}
	aad := []byte(registration + "\x00" + a.ID + "\x00" + a.Username)
	if open {
		raw, e := base64.RawStdEncoding.DecodeString(value)
		if e != nil || len(raw) < g.NonceSize() {
			return "", invalid
		}
		out, e := g.Open(nil, raw[:g.NonceSize()], raw[g.NonceSize():], aad)
		if e != nil {
			return "", invalid
		}
		return string(out), nil
	}
	nonce := make([]byte, g.NonceSize())
	if _, e = rand.Read(nonce); e != nil {
		return "", invalid
	}
	return base64.RawStdEncoding.EncodeToString(g.Seal(nonce, nonce, []byte(value), aad)), nil
}
func client(address, username, password string) (*kgo.Client, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(address), kgo.RequestRetries(0), kgo.DialTimeout(5 * time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.Dialer(func(ctx context.Context, network, target string) (net.Conn, error) {
		if target != address {
			return nil, invalid
		}
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, target)
	})}
	if username != "" {
		opts = append(opts, kgo.SASL(scram.Auth{User: username, Pass: password}.AsSha256Mechanism()))
	}
	return kgo.NewClient(opts...)
}
func users(ctx context.Context, address string) ([]string, error) {
	cl, e := client(address, "", "")
	if e != nil {
		return nil, invalid
	}
	defer cl.Close()
	r, e := kmsg.NewPtrDescribeUserSCRAMCredentialsRequest().RequestWith(ctx, cl)
	if e != nil || r.ErrorCode != 0 {
		return nil, invalid
	}
	out := []string{}
	for _, u := range r.Results {
		if u.ErrorCode != 0 {
			return nil, invalid
		}
		for _, c := range u.CredentialInfos {
			if c.Mechanism == 1 && strings.HasPrefix(u.User, "lo_") {
				out = append(out, u.User)
				break
			}
		}
	}
	if len(out) > 8 {
		return nil, invalid
	}
	sort.Strings(out)
	return out, nil
}
func verify(ctx context.Context, address, username, password string) error {
	if len(password) == 0 || len(password) > 4096 || strings.ContainsRune(password, 0) {
		return invalid
	}
	cl, e := client(address, username, password)
	if e != nil {
		return invalid
	}
	defer cl.Close()
	r, e := kmsg.NewPtrApiVersionsRequest().RequestWith(ctx, cl)
	if e != nil || r.ErrorCode != 0 {
		return invalid
	}
	return nil
}
func registration(ctx context.Context, db *sql.DB, id string) (registry.Registration, error) {
	var raw []byte
	var r registry.Registration
	if e := db.QueryRowContext(ctx, `SELECT payload FROM kafka_cluster_registrations WHERE id=$1 AND deleted_at IS NULL`, id).Scan(&raw); e != nil {
		return r, invalid
	}
	if json.Unmarshal(raw, &r) != nil || r.ID != id || !allowed(r) {
		return registry.Registration{}, invalid
	}
	return r, nil
}

type candidate struct {
	registration string
	account      workspace.Account
	secret       workspace.AccountSecret
}

func saved(st workspace.State, name string) bool {
	for _, a := range st.Accounts {
		if a.Username == name {
			if secret, ok := st.AccountSecrets[a.ID]; ok && secret.Ciphertext != "" {
				return true
			}
		}
	}
	return false
}
func importOne(ctx context.Context, store workspace.Store, id, name, password string, key []byte) (bool, error) {
	imported := false
	e := store.Update(ctx, id, func(st *workspace.State) error {
		if saved(*st, name) {
			return nil
		}
		a := workspace.Account{ID: uuid.NewString(), Username: name, Principal: "User:" + name, Mechanism: "SCRAM-SHA-256", Source: "import", CredentialState: "managed", Observation: "verified", Revision: 1, ApplicationIDs: []string{}}
		// Keep account associations when completing a password-less reference.
		for oldID, old := range st.Accounts {
			if old.Username == name {
				a.ApplicationIDs = old.ApplicationIDs
				a.Revision = old.Revision + 1
				delete(st.Accounts, oldID)
				delete(st.AccountSecrets, oldID)
			}
		}
		ciphertext, e := crypt(key, id, a, password, false)
		if e != nil {
			return e
		}
		if st.Accounts == nil {
			st.Accounts = map[string]workspace.Account{}
		}
		if st.AccountSecrets == nil {
			st.AccountSecrets = map[string]workspace.AccountSecret{}
		}
		st.Accounts[a.ID] = a
		st.AccountSecrets[a.ID] = workspace.AccountSecret{Ciphertext: ciphertext, KeyVersion: 1}
		st.Audit = append(st.Audit, workspace.Audit{Actor: "lab-maintenance", Action: "account.import", Target: a.ID, At: time.Now().UTC()})
		imported = true
		return nil
	})
	return imported, e
}
func run(ctx context.Context, in input, key []byte) (counts, error) {
	var out counts
	db, e := registry.OpenDevelopmentDatabase(in.DatabaseURL)
	if e != nil {
		return out, invalid
	}
	defer db.Close()
	target, e := registration(ctx, db, in.RegistrationID)
	if e != nil {
		return out, invalid
	}
	// Restrict SQL before reading workspace records; never decrypt non-lab data.
	rows, e := db.QueryContext(ctx, `SELECT payload FROM kafka_cluster_registrations WHERE deleted_at IS NULL AND payload->>'Environment'=$1 AND payload->>'SecurityProtocol'='PLAINTEXT' AND payload->'BootstrapServers'=$2::jsonb`, "实验环境", mustJSON(target.BootstrapServers))
	if e != nil {
		return out, invalid
	}
	regs := []registry.Registration{}
	for rows.Next() {
		var raw []byte
		var r registry.Registration
		if rows.Scan(&raw) != nil || json.Unmarshal(raw, &r) != nil {
			rows.Close()
			return out, invalid
		}
		if allowed(r) {
			regs = append(regs, r)
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return out, invalid
	}
	store := workspace.NewPostgresStore(db)
	candidates := map[string][]candidate{}
	for _, r := range regs {
		st, e := store.View(ctx, r.ID)
		if e != nil {
			return out, invalid
		}
		for _, a := range st.Accounts {
			secret, ok := st.AccountSecrets[a.ID]
			if ok && secret.KeyVersion == 1 && a.Source == "create" && a.Mechanism == "SCRAM-SHA-256" && strings.HasPrefix(a.Username, "lo_") && len(candidates[a.Username]) < 8 {
				candidates[a.Username] = append(candidates[a.Username], candidate{r.ID, a, secret})
			}
		}
	}
	queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	names, e := users(queryCtx, target.BootstrapServers[0])
	cancel()
	if e != nil {
		return out, invalid
	}
	listener := strings.TrimSuffix(target.BootstrapServers[0], ":9092") + ":9094"
	for _, name := range names {
		st, e := store.View(ctx, in.RegistrationID)
		if e != nil {
			return out, invalid
		}
		if saved(st, name) {
			out.Existing++
			continue
		}
		if len(candidates[name]) == 0 {
			out.Missing++
			continue
		}
		authenticated := false
		for _, c := range candidates[name] {
			password, e := crypt(key, c.registration, c.account, c.secret.Ciphertext, true)
			if e != nil {
				continue
			}
			verifyCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
			e = verify(verifyCtx, listener, name, password)
			cancel()
			if e != nil {
				continue
			}
			current, e := registration(ctx, db, in.RegistrationID)
			if e != nil || current.BootstrapServers[0] != target.BootstrapServers[0] {
				return out, invalid
			}
			imported, e := importOne(ctx, store, in.RegistrationID, name, password, key)
			if e != nil {
				return out, invalid
			}
			if imported {
				out.Imported++
			} else {
				out.Existing++
			}
			authenticated = true
			break
		}
		if !authenticated {
			out.Failed++
		}
	}
	return out, nil
}
func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
func main() {
	lab := flag.Bool("lab", false, "enable isolated lab password import")
	flag.Parse()
	in, key, e := readInput(os.Stdin, *lab)
	if e == nil && flag.NArg() != 0 {
		e = invalid
	}
	var out counts
	if e == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		out, e = run(ctx, in, key)
		cancel()
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, "LAB_IMPORT_FAILED")
		os.Exit(1)
	}
	_ = json.NewEncoder(os.Stdout).Encode(out)
}
