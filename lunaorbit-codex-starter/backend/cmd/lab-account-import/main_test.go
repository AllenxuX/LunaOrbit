package main

import (
	"context"
	"encoding/json"
	"lunaorbit/backend/internal/registry"
	"lunaorbit/backend/internal/workspace"
	"strings"
	"testing"
)

func validInput() input {
	return input{DatabaseURL: "postgres://synthetic:synthetic@127.0.0.1:5432/lunaorbit_phase02_test?sslmode=disable", AccountMasterKey: strings.Repeat("ab", 32), RegistrationID: strings.Repeat("a", 32)}
}
func TestInputGuards(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*input)
	}{
		{"remote", func(i *input) { i.DatabaseURL = "postgres://u:p@192.0.2.1:5432/lunaorbit_phase02_test" }},
		{"wrong database", func(i *input) { i.DatabaseURL = "postgres://u:p@127.0.0.1:5432/production" }},
		{"wrong port", func(i *input) { i.DatabaseURL = "postgres://u:p@127.0.0.1:5433/lunaorbit_phase02_test" }},
		{"query host", func(i *input) { i.DatabaseURL += "&host=192.0.2.1" }},
		{"zero key", func(i *input) { i.AccountMasterKey = strings.Repeat("00", 32) }},
		{"invalid id", func(i *input) { i.RegistrationID = "invalid" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := validInput()
			c.mutate(&in)
			b, _ := json.Marshal(in)
			if _, _, e := readInput(strings.NewReader(string(b)), true); e == nil {
				t.Fatal("unsafe input accepted")
			}
		})
	}
	b, _ := json.Marshal(validInput())
	for _, s := range []string{string(b) + "{}", strings.TrimSuffix(string(b), "}") + `,"extra":true}`, strings.Repeat(" ", 16385)} {
		if _, _, e := readInput(strings.NewReader(s), true); e == nil {
			t.Fatal("malformed input accepted")
		}
	}
	if _, _, e := readInput(strings.NewReader(string(b)), false); e == nil {
		t.Fatal("missing opt-in accepted")
	}
	if _, key, e := readInput(strings.NewReader(string(b)), true); e != nil || len(key) != 32 {
		t.Fatal("valid lab input refused")
	}
}
func TestRegistrationGuards(t *testing.T) {
	r := registry.Registration{Enabled: true, Environment: "实验环境", SecurityProtocol: "PLAINTEXT", BootstrapServers: []string{"kafka-target.lunaorbit-integration.svc.cluster.local:9092"}}
	if !allowed(r) {
		t.Fatal("lab target refused")
	}
	for _, change := range []func(*registry.Registration){func(r *registry.Registration) { r.Environment = "production" }, func(r *registry.Registration) { r.SecurityProtocol = "SASL_PLAINTEXT" }, func(r *registry.Registration) { r.BootstrapServers = append(r.BootstrapServers, "other:9092") }, func(r *registry.Registration) { r.BootstrapServers = []string{"other:9092"} }, func(r *registry.Registration) { r.Enabled = false }} {
		v := r
		change(&v)
		if allowed(v) {
			t.Fatal("non-lab registration accepted")
		}
	}
}
func TestImportReencryptsAndPreservesExisting(t *testing.T) {
	ctx := context.Background()
	store := workspace.NewMemoryStore()
	key := []byte(strings.Repeat("x", 32))
	source := workspace.Account{ID: "source-id", Username: "lo_synthetic"}
	encrypted, e := crypt(key, "source", source, "synthetic-password", false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = crypt(key, "target", source, encrypted, true); e == nil {
		t.Fatal("source AAD accepted for another registration")
	}
	password, e := crypt(key, "source", source, encrypted, true)
	if e != nil {
		t.Fatal(e)
	}
	if e = store.Update(ctx, "target", func(st *workspace.State) error {
		st.Accounts["reference"] = workspace.Account{ID: "reference", Username: source.Username, ApplicationIDs: []string{"application"}, Revision: 3}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	ok, e := importOne(ctx, store, "target", source.Username, password, key)
	if e != nil || !ok {
		t.Fatal("import failed")
	}
	st, _ := store.View(ctx, "target")
	if len(st.Accounts) != 1 || len(st.Audit) != 1 {
		t.Fatal("wrong account/audit count")
	}
	var imported workspace.Account
	for _, a := range st.Accounts {
		imported = a
	}
	if imported.ID == source.ID || imported.ID == "reference" || imported.Source != "import" || imported.CredentialState != "managed" || imported.Observation != "verified" || imported.Revision != 4 || len(imported.ApplicationIDs) != 1 {
		t.Fatal("incorrect imported account")
	}
	secret := st.AccountSecrets[imported.ID]
	if secret.Ciphertext == encrypted {
		t.Fatal("ciphertext reused")
	}
	value, e := crypt(key, "target", imported, secret.Ciphertext, true)
	if e != nil || value != password {
		t.Fatal("target secret unreadable")
	}
	if st.Audit[0].Actor != "lab-maintenance" || st.Audit[0].Action != "account.import" {
		t.Fatal("audit missing")
	}
	ok, e = importOne(ctx, store, "target", source.Username, "different-synthetic-password", key)
	if e != nil || ok {
		t.Fatal("existing password overwritten")
	}
	next, _ := store.View(ctx, "target")
	if next.AccountSecrets[imported.ID] != secret || len(next.Audit) != 1 {
		t.Fatal("idempotent import changed saved secret")
	}
	raw, _ := json.Marshal(st.Audit)
	if strings.Contains(string(raw), password) {
		t.Fatal("password leaked into audit")
	}
}
