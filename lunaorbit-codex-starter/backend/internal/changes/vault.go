package changes

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

type Vault struct{ aead cipher.AEAD }

func NewVault(key []byte) (*Vault, error) {
	if len(key) != 32 {
		return nil, errors.New("INVALID_ENCRYPTION_KEY")
	}
	block, e := aes.NewCipher(key)
	if e != nil {
		return nil, errors.New("INVALID_ENCRYPTION_KEY")
	}
	a, e := cipher.NewGCM(block)
	if e != nil {
		return nil, errors.New("INVALID_ENCRYPTION_KEY")
	}
	return &Vault{a}, nil
}
func (v *Vault) Seal(raw, aad []byte) ([]byte, error) {
	if v == nil || len(raw) > maxSnapshot-64 {
		return nil, errors.New("SNAPSHOT_UNAVAILABLE")
	}
	nonce := make([]byte, v.aead.NonceSize())
	if _, e := io.ReadFull(rand.Reader, nonce); e != nil {
		return nil, errors.New("SNAPSHOT_UNAVAILABLE")
	}
	return v.aead.Seal(nonce, nonce, raw, aad), nil
}
func (v *Vault) Open(data, aad []byte) ([]byte, error) {
	if v == nil || len(data) < v.aead.NonceSize() {
		return nil, errors.New("SNAPSHOT_AUTHENTICATION_FAILED")
	}
	n := v.aead.NonceSize()
	out, e := v.aead.Open(nil, data[:n], data[n:], aad)
	if e != nil {
		return nil, errors.New("SNAPSHOT_AUTHENTICATION_FAILED")
	}
	return out, nil
}
