package changes

import (
	"bytes"
	"testing"
)

func TestVaultAuthenticatesAADAndKey(t *testing.T) {
	v, e := NewVault(bytes.Repeat([]byte{1}, 32))
	if e != nil {
		t.Fatal(e)
	}
	cipher, e := v.Seal([]byte("DO_NOT_EXPOSE"), []byte("owner/target/request/uid/rv"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = v.Open(cipher, []byte("other-owner")); e == nil {
		t.Fatal("AAD not authenticated")
	}
	other, _ := NewVault(bytes.Repeat([]byte{2}, 32))
	if _, e = other.Open(cipher, []byte("owner/target/request/uid/rv")); e == nil {
		t.Fatal("wrong key accepted")
	}
}
