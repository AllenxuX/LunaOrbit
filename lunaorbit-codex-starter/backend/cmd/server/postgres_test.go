package main

import (
	"bytes"
	"log/slog"
	"lunaorbit/backend/internal/config"
	"strings"
	"testing"
	"time"
)

func TestReleaseDatabaseUnreachableFailsStartupWithoutPassword(t *testing.T) {
	old := registryDatabaseWait
	registryDatabaseWait = 300 * time.Millisecond
	defer func() { registryDatabaseWait = old }()
	var logs bytes.Buffer
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(oldLogger)
	cfg := config.Config{RegistryMode: "postgres", RuntimeMode: "readonly-lab", DatabaseURL: "postgresql://u:synthetic-secret-pw@127.0.0.1:1/lunaorbit?sslmode=disable"}
	db, e := openRegistryDatabase(cfg)
	if db != nil || e == nil || e.Error() != "registry database unavailable" {
		t.Fatalf("%v %v", db, e)
	}
	if strings.Contains(logs.String(), "synthetic-secret-pw") || strings.Contains(e.Error(), "synthetic-secret-pw") {
		t.Fatal("password leaked")
	}
}
