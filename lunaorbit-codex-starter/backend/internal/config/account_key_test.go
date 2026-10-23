package config

import (
	"strings"
	"testing"
)

func TestAccountMasterKeyValidation(t *testing.T) {
	for _, key := range []string{"bad", strings.Repeat("00", 31), strings.Repeat("00", 33)} {
		if _, e := Load(func(k string) string {
			if k == "ACCOUNT_MASTER_KEY" {
				return key
			}
			return ""
		}); e == nil {
			t.Fatal("invalid account key accepted")
		}
	}
	if _, e := Load(func(k string) string {
		if k == "ACCOUNT_MASTER_KEY" {
			return strings.Repeat("a1", 32)
		}
		return ""
	}); e != nil {
		t.Fatal(e)
	}
}
