package registry

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

var (
	errInvalidDatabase     = errors.New("invalid registry database configuration")
	errUnavailableDatabase = errors.New("registry database unavailable")
)

// ParseDatabaseURL validates the release DATABASE_URL (REGISTRY_MODE=postgres): a postgres URL with an
// explicit host and database name. Service hostnames and ordinary database names are allowed.
// Errors never echo the URL, so the password cannot reach logs.
func ParseDatabaseURL(dsn string) (*pgx.ConnConfig, error) {
	u, e := url.Parse(dsn)
	if e != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Fragment != "" || u.Hostname() == "" || strings.Trim(u.Path, "/") == "" {
		return nil, errInvalidDatabase
	}
	cfg, e := pgx.ParseConfig(dsn)
	if e != nil || cfg.Host == "" || cfg.Database == "" {
		return nil, errInvalidDatabase
	}
	return cfg, nil
}

// OpenDatabase opens and pings the release registry database. It never returns driver errors.
func OpenDatabase(dsn string) (*sql.DB, error) {
	cfg, e := ParseDatabaseURL(dsn)
	if e != nil {
		return nil, e
	}
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e = db.PingContext(ctx); e != nil {
		db.Close()
		return nil, errUnavailableDatabase
	}
	return db, nil
}
