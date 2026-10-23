package identity

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
)

type Subject struct {
	ID     string   `json:"user_id"`
	Role   string   `json:"role"`
	Groups []string `json:"groups"`
}
type Provider interface {
	Authenticate(context.Context, string, string) (Subject, error)
}
type SecretReader interface {
	Get(context.Context, string) ([]byte, error)
}

var ErrCredentials = errors.New("INVALID_CREDENTIALS")
var ErrUnavailable = errors.New("IDENTITY_UNAVAILABLE")
var ErrConfig = errors.New("INVALID_IDENTITY_CONFIGURATION")

func validRole(role string) bool {
	return role == "admin" || role == "operator" || role == "viewer" || role == "security-admin"
}
func stableID(realm, subject string) string {
	v := sha256.Sum256([]byte(realm + "\x00" + subject))
	return hex.EncodeToString(v[:])
}

type MockUser struct {
	Username, Password string
	Subject            Subject
}
type mockUser struct {
	password [32]byte
	subject  Subject
}
type MockProvider struct{ users map[string]mockUser }

func NewMockProvider(users []MockUser) (*MockProvider, error) {
	p := &MockProvider{users: map[string]mockUser{}}
	for _, u := range users {
		if u.Username == "" || u.Password == "" || u.Subject.ID == "" || !validRole(u.Subject.Role) {
			return nil, ErrConfig
		}
		if _, ok := p.users[u.Username]; ok {
			return nil, ErrConfig
		}
		u.Subject.Groups = append([]string(nil), u.Subject.Groups...)
		p.users[u.Username] = mockUser{sha256.Sum256([]byte(u.Password)), u.Subject}
	}
	return p, nil
}
func (p *MockProvider) Authenticate(ctx context.Context, user, password string) (Subject, error) {
	if ctx.Err() != nil {
		return Subject{}, ErrUnavailable
	}
	u, ok := p.users[user]
	h := sha256.Sum256([]byte(password))
	if !ok || password == "" || subtle.ConstantTimeCompare(h[:], u.password[:]) != 1 {
		return Subject{}, ErrCredentials
	}
	s := u.subject
	s.Groups = append([]string(nil), s.Groups...)
	return s, nil
}
