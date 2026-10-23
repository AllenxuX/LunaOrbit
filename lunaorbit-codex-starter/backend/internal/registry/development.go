package registry

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// OpenDevelopmentDatabase cannot target a non-loopback or ordinary business DB.
// It never returns connection configuration or a driver error to the caller.
func OpenDevelopmentDatabase(dsn string) (*sql.DB, error) {
	bad := errors.New("invalid development database configuration")
	u, e := url.Parse(dsn)
	if e != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Fragment != "" {
		return nil, bad
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() || (u.Path != "/lunaorbit_phase00_test" && u.Path != "/lunaorbit_phase02_test") {
		return nil, bad
	}
	query, e := url.ParseQuery(u.RawQuery)
	if e != nil {
		return nil, bad
	}
	for key, values := range query {
		if key != "sslmode" || len(values) != 1 {
			return nil, bad
		}
	}
	cfg, e := pgx.ParseConfig(dsn)
	if e != nil || cfg.Host != u.Hostname() || cfg.Database != strings.TrimPrefix(u.Path, "/") || len(cfg.RuntimeParams) != 0 {
		return nil, bad
	}
	for _, fallback := range cfg.Fallbacks {
		if fallback.Host != cfg.Host {
			return nil, bad
		}
	}
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e = db.PingContext(ctx); e != nil {
		db.Close()
		return nil, errors.New("development database unavailable")
	}
	return db, nil
}
