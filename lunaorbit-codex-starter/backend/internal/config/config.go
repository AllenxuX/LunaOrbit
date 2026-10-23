package config

import (
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

type Config struct {
	PlatformMode, InformationWorkspaceID                                                                                                      string
	InformationAllowedHosts, InformationBrowserOrigins                                                                                        []string
	AccountMasterKey                                                                                                                          string
	Address, LogLevel, RegistryMode, DatabaseURL, DevSessionKey, DevBrowserOrigin, DiscoveryMode, RuntimeMode, ChangesMode, ObservabilityMode string
	KiteWrites, KafkaWrites                                                                                                                   bool
}

func Load(getenv func(string) string) (Config, error) {
	c := Config{Address: getenv("HTTP_ADDR"), LogLevel: getenv("LOG_LEVEL")}
	c.PlatformMode = getenv("PLATFORM_MODE")
	if c.PlatformMode != "" && c.PlatformMode != "information" {
		return Config{}, errors.New("invalid PLATFORM_MODE")
	}
	if c.PlatformMode == "information" {
		c.InformationWorkspaceID = getenv("INFORMATION_WORKSPACE_ID")
		if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`).MatchString(c.InformationWorkspaceID) {
			return Config{}, errors.New("explicit INFORMATION_WORKSPACE_ID required")
		}
		for _, h := range strings.Split(getenv("INFORMATION_ALLOWED_HOSTS"), ",") {
			if h != "" {
				if strings.ContainsAny(h, "/*@?# ") {
					return Config{}, errors.New("invalid INFORMATION_ALLOWED_HOSTS")
				}
				c.InformationAllowedHosts = append(c.InformationAllowedHosts, h)
			}
		}
		for _, o := range strings.Split(getenv("INFORMATION_BROWSER_ORIGINS"), ",") {
			if o != "" {
				u, e := url.Parse(o)
				if e != nil || u.User != nil || u.Host == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") || strings.Contains(o, "*") {
					return Config{}, errors.New("invalid INFORMATION_BROWSER_ORIGINS")
				}
				c.InformationBrowserOrigins = append(c.InformationBrowserOrigins, o)
			}
		}
		if v := getenv("CHANGES_MODE"); v != "" && v != "disabled" {
			return Config{}, errors.New("information mode disables changes")
		}
		if getenv("ENABLE_KAFKA_WRITES") == "true" || getenv("ENABLE_KITE_WRITES") == "true" {
			return Config{}, errors.New("information mode disables infrastructure writes")
		}
	}
	c.AccountMasterKey = getenv("ACCOUNT_MASTER_KEY")
	if c.AccountMasterKey != "" {
		key, err := hex.DecodeString(c.AccountMasterKey)
		if err != nil || len(key) != 32 {
			return Config{}, errors.New("ACCOUNT_MASTER_KEY must be 32 bytes encoded as hex")
		}
	}
	if c.Address == "" {
		c.Address = "127.0.0.1:8080"
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	c.RuntimeMode = getenv("LUNAORBIT_RUNTIME_MODE")
	if c.RuntimeMode != "" && c.RuntimeMode != "readonly-lab" && c.RuntimeMode != "admin-lab" {
		return Config{}, errors.New("invalid LUNAORBIT_RUNTIME_MODE")
	}
	c.RegistryMode = getenv("REGISTRY_MODE")
	if c.RegistryMode == "" {
		c.RegistryMode = "disabled"
	}
	if c.RegistryMode != "disabled" && c.RegistryMode != "mock" && c.RegistryMode != "postgres-mock" {
		return Config{}, errors.New("invalid REGISTRY_MODE")
	}
	identityMode := getenv("IDENTITY_MODE")
	if c.PlatformMode == "information" {
		identityMode = ""
	}
	if identityMode != "" && identityMode != "dev" && identityMode != "mock" && identityMode != "ldap" {
		return Config{}, errors.New("invalid IDENTITY_MODE")
	}
	if identityMode != "" && identityMode != "dev" && c.RegistryMode == "disabled" {
		return Config{}, errors.New("identity requires explicit registry mode")
	}
	c.DiscoveryMode = getenv("DISCOVERY_MODE")
	if c.DiscoveryMode == "" {
		c.DiscoveryMode = "disabled"
	}
	if c.DiscoveryMode != "disabled" && c.DiscoveryMode != "mock" && c.DiscoveryMode != "readonly-lab" {
		return Config{}, errors.New("invalid DISCOVERY_MODE")
	}
	if c.DiscoveryMode != "disabled" && c.RegistryMode == "disabled" {
		return Config{}, errors.New("discovery mock requires explicit registry development mode")
	}
	if c.RuntimeMode == "readonly-lab" || c.RuntimeMode == "admin-lab" {
		if (c.RegistryMode != "mock" && c.RegistryMode != "postgres-mock") || c.DiscoveryMode != "readonly-lab" {
			return Config{}, errors.New("lab requires guarded development registry and lab discovery")
		}
	} else if c.DiscoveryMode == "readonly-lab" {
		return Config{}, errors.New("lab discovery requires explicit runtime mode")
	}
	observationMode := getenv("OBSERVABILITY_MODE")
	c.ObservabilityMode = observationMode
	if observationMode != "" && observationMode != "disabled" && observationMode != "mock" {
		return Config{}, errors.New("invalid OBSERVABILITY_MODE")
	}
	if observationMode == "mock" && c.DiscoveryMode != "mock" {
		return Config{}, errors.New("observability mock requires synthetic discovery")
	}
	changesMode := getenv("CHANGES_MODE")
	c.ChangesMode = changesMode
	if changesMode != "" && changesMode != "disabled" && changesMode != "mock" && changesMode != "lab" {
		return Config{}, errors.New("invalid CHANGES_MODE")
	}
	if changesMode == "mock" && c.DiscoveryMode != "mock" {
		return Config{}, errors.New("changes mock requires synthetic discovery")
	}
	if changesMode == "lab" && c.RuntimeMode != "admin-lab" {
		return Config{}, errors.New("lab changes require admin-lab")
	}
	if c.RegistryMode != "disabled" {
		c.DevBrowserOrigin = getenv("DEV_BROWSER_ORIGIN")
		if c.DevBrowserOrigin == "" {
			c.DevBrowserOrigin = "http://127.0.0.1:5173"
		}
		host, _, splitErr := net.SplitHostPort(c.Address)
		ip := net.ParseIP(host)
		if (splitErr != nil || ip == nil || !ip.IsLoopback()) && !(c.PlatformMode == "information" && len(c.InformationAllowedHosts) > 0) {
			return Config{}, errors.New("registry development mode requires loopback HTTP_ADDR")
		}
	}
	if c.RegistryMode == "postgres-mock" {
		c.DatabaseURL = getenv("DATABASE_URL")
		if c.DatabaseURL == "" {
			return Config{}, errors.New("DATABASE_URL required for postgres-mock")
		}
		if c.PlatformMode != "information" {
			c.DevSessionKey = getenv("DEV_SESSION_KEY")
			key, e := hex.DecodeString(c.DevSessionKey)
			if e != nil || len(key) != 32 {
				return Config{}, errors.New("DEV_SESSION_KEY must be 32 bytes encoded as hex")
			}
		}
	}
	_, port, err := net.SplitHostPort(c.Address)
	n, parseErr := strconv.Atoi(port)
	if err != nil || parseErr != nil || n < 1 || n > 65535 {
		return Config{}, errors.New("invalid HTTP_ADDR")
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return Config{}, errors.New("invalid LOG_LEVEL")
	}
	for _, key := range []string{"ENABLE_KITE_WRITES", "ENABLE_KAFKA_WRITES"} {
		value := getenv(key)
		if value != "" && value != "false" && !(value == "true" && c.RuntimeMode == "admin-lab") {
			return Config{}, errors.New(key + " must remain false")
		}
	}
	c.KiteWrites = getenv("ENABLE_KITE_WRITES") == "true"
	c.KafkaWrites = getenv("ENABLE_KAFKA_WRITES") == "true"
	return c, nil
}
