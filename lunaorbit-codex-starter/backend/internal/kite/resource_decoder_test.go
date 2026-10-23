package kite

import (
	"errors"
	"testing"
)

type resourceDecoderFunc func(string, bool, []byte, any) error

func (f resourceDecoderFunc) DecodeResource(k string, s bool, b []byte, target any) error {
	return f(k, s, b, target)
}
func TestResourceDecoderBoundary(t *testing.T) {
	d := Document{kind: "configmaps", body: []byte("secret")}
	if CodeOf(d.DecodeResource(nil, nil)) != "RESPONSE_FORMAT_UNVERIFIED" {
		t.Fatal("nil must fail closed")
	}
	bad := resourceDecoderFunc(func(_ string, _ bool, b []byte, _ any) error { b[0] = 'X'; return errors.New("secret") })
	if CodeOf(d.DecodeResource(bad, nil)) != "INVALID_RESPONSE" {
		t.Fatal("unsafe decoder error")
	}
	if string(d.body) != "secret" {
		t.Fatal("decoder mutated document")
	}
}
