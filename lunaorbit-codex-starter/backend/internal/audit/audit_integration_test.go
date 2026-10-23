package audit

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// This opt-in test only accepts a dedicated Phase 00 laboratory database.
// It never prints the DSN and never targets an existing business database.
func TestSQLSinkPostgres(t *testing.T) {
	dsn := os.Getenv("LUNAORBIT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("dedicated Phase 00 PostgreSQL test database not configured")
	}
	if err := validateLabDSN(dsn); err != nil {
		t.Fatal("integration test requires a loopback Phase 00 laboratory database")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal("could not initialize laboratory database client")
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal("laboratory database unavailable")
	}
	actor := "synthetic-phase00-" + time.Now().UTC().Format("20060102T150405.000000000")
	event := Event{ActorID: actor, Action: "foundation.verify", TargetID: "synthetic-resource", Outcome: "success"}
	if err := (SQLSink{DB: db}).Record(ctx, event); err != nil {
		t.Fatal(err)
	}
	var action, target, outcome string
	if err := db.QueryRowContext(ctx, `SELECT action, target_id, outcome FROM audit_events WHERE actor_id = $1`, actor).Scan(&action, &target, &outcome); err != nil {
		t.Fatal("could not read persisted synthetic audit event")
	}
	if action != event.Action || target != event.TargetID || outcome != event.Outcome {
		t.Fatal("persisted audit event differs from input")
	}
	// Exercise a real database constraint failure without exposing driver details.
	event.ActorID = strings.Repeat("x", 129)
	if err := (SQLSink{DB: db}).Record(ctx, event); err == nil || err.Error() != "audit persistence failed" {
		t.Fatal("database failure must return a safe audit error")
	}
}

func validateLabDSN(dsn string) error {
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() != "127.0.0.1" || !strings.HasPrefix(u.Path, "/lunaorbit_phase00_") || u.Fragment != "" {
		return errors.New("dedicated laboratory database required")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return errors.New("invalid laboratory connection options")
	}
	// pgx query options can override the URL host/database. Allow only sslmode,
	// with one known value; never pass arbitrary driver options through this gate.
	for key, values := range query {
		if key != "sslmode" || len(values) != 1 {
			return errors.New("unsupported laboratory connection option")
		}
		switch values[0] {
		case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		default:
			return errors.New("invalid laboratory TLS mode")
		}
	}
	return nil
}

func TestLaboratoryDSNRejectsTargetOverrides(t *testing.T) {
	base := "postgresql://127.0.0.1/lunaorbit_phase00_test"
	for _, query := range []string{"host=remote.example", "hostaddr=192.0.2.1", "dbname=business", "database=business", "service=business", "sslmode=disable&host=remote.example", "h%6fst=remote.example", "sslmode=disable&sslmode=require"} {
		t.Run(query, func(t *testing.T) {
			if validateLabDSN(base+"?"+query) == nil {
				t.Fatal("unsafe connection-string query accepted")
			}
		})
	}
}

func TestLaboratoryDSNAcceptsDedicatedLoopback(t *testing.T) {
	for _, dsn := range []string{"postgresql://127.0.0.1/lunaorbit_phase00_test", "postgresql://synthetic-user:synthetic-password@127.0.0.1:25432/lunaorbit_phase00_test?sslmode=disable"} {
		if err := validateLabDSN(dsn); err != nil {
			t.Fatal("valid laboratory DSN rejected")
		}
	}
}
