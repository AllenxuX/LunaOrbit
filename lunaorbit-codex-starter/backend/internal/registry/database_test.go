package registry

import (
	"strings"
	"testing"
)

func TestParseDatabaseURLAcceptsServiceHostAndNormalDatabase(t *testing.T) {
	cfg, e := ParseDatabaseURL("postgresql://lunaorbit:synthetic-pass@lunaorbit-postgres.lunaorbit.svc.cluster.local:5432/lunaorbit?sslmode=disable")
	if e != nil {
		t.Fatal(e)
	}
	if cfg.Host != "lunaorbit-postgres.lunaorbit.svc.cluster.local" || cfg.Database != "lunaorbit" || cfg.Port != 5432 {
		t.Fatalf("unexpected config %s %s %d", cfg.Host, cfg.Database, cfg.Port)
	}
}

func TestParseDatabaseURLRejectsUnparseableOrIncomplete(t *testing.T) {
	for _, dsn := range []string{"", "synthetic-invalid", "mysql://u:p@db/lunaorbit", "postgres://u:p@/lunaorbit", "postgres://u:p@db", "postgres://u:p@db/lunaorbit#frag", "postgres://u:p@db:notaport/lunaorbit"} {
		if _, e := ParseDatabaseURL(dsn); e == nil {
			t.Fatalf("accepted %q", dsn)
		}
	}
}

func TestDevelopmentDatabaseStillRejectsServiceHostAndNormalDatabase(t *testing.T) {
	for _, dsn := range []string{
		"postgresql://u:p@lunaorbit-postgres.lunaorbit.svc.cluster.local:5432/lunaorbit_phase02_test?sslmode=disable",
		"postgresql://u:p@127.0.0.1:5432/lunaorbit?sslmode=disable",
	} {
		if _, e := OpenDevelopmentDatabase(dsn); e == nil || e.Error() != "invalid development database configuration" {
			t.Fatalf("%q: %v", dsn, e)
		}
	}
}

func TestOpenDatabaseFailsClearlyWithoutLeakingPassword(t *testing.T) {
	_, e := OpenDatabase("postgresql://lunaorbit:synthetic-secret-pw@127.0.0.1:1/lunaorbit?sslmode=disable")
	if e == nil || e.Error() != "registry database unavailable" {
		t.Fatalf("got %v", e)
	}
	_, e = OpenDatabase("postgres://lunaorbit:synthetic-secret-pw@/x")
	if e == nil || e.Error() != "invalid registry database configuration" || strings.Contains(e.Error(), "synthetic-secret-pw") {
		t.Fatalf("got %v", e)
	}
}
