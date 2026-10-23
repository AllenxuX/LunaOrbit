// Package kite provides only the read routes in KITE_API_CONTRACT.md.
// It does not assume a Kite response envelope or expose ConfigMap bodies to the UI.
package kite

import (
	"context"
	"errors"
	"fmt"
)

type KiteClient interface {
	ListNamespaces(context.Context, string) (Document, error)
	ListConfigMaps(context.Context, string, string) (Document, error)
	GetConfigMap(context.Context, string, string, string) (Document, error)
	ListDeployments(context.Context, string, string) (Document, error)
	GetDeployment(context.Context, string, string, string) (Document, error)
	ListPods(context.Context, string, string) (Document, error)
	GetPod(context.Context, string, string, string) (Document, error)
}

// Error intentionally retains neither upstream bodies nor underlying URL errors.
type Error struct {
	Code       string `json:"code"`
	HTTPStatus int    `json:"http_status,omitempty"`
}

func (e *Error) Error() string { return "kite: " + e.Code }
func CodeOf(err error) string {
	var failure *Error
	if errors.As(err, &failure) {
		return failure.Code
	}
	if err != nil {
		return "INTERNAL_ERROR"
	}
	return ""
}

// Document keeps raw data server-side and is safe to format or JSON-encode.
// Decode must be explicitly supplied with an implementation verified for the
// deployed Kite version. A nil decoder never guesses the envelope.
type Document struct {
	kind   string
	single bool
	body   []byte
}

type Metadata struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}
type Snapshot struct {
	Items []Metadata `json:"items"`
}
type Decoder interface {
	Decode(kind string, single bool, body []byte) (Snapshot, error)
}

func (d Document) Decode(decoder Decoder) (Snapshot, error) {
	if decoder == nil {
		return Snapshot{}, &Error{Code: "RESPONSE_FORMAT_UNVERIFIED"}
	}
	// The decoder owns its copy, not the transport's retained bytes.
	result, err := decoder.Decode(d.kind, d.single, append([]byte(nil), d.body...))
	if err != nil {
		return Snapshot{}, &Error{Code: "INVALID_RESPONSE"}
	}
	return result, nil
}
func (d Document) String() string                    { return "kite.Document[redacted]" }
func (d Document) GoString() string                  { return d.String() }
func (d Document) Format(state fmt.State, verb rune) { _, _ = state.Write([]byte(d.String())) }
func (d Document) MarshalJSON() ([]byte, error) {
	return []byte(`{"content":"redacted","format":"unverified"}`), nil
}

// ResourceDecoder is an explicitly verified adapter from a deployed Kite envelope
// into a server-only Kubernetes projection. It never implies an envelope contract.
type ResourceDecoder interface {
	DecodeResource(kind string, single bool, body []byte, target any) error
}

func (d Document) DecodeResource(decoder ResourceDecoder, target any) error {
	if decoder == nil {
		return &Error{Code: "RESPONSE_FORMAT_UNVERIFIED"}
	}
	if err := decoder.DecodeResource(d.kind, d.single, append([]byte(nil), d.body...), target); err != nil {
		return &Error{Code: "INVALID_RESPONSE"}
	}
	return nil
}
