package kafkaadmin

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/crypto/pbkdf2"
	"io"
	"lunaorbit/backend/internal/kafkaops"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func scramBroker(t *testing.T, code int16, saslMode ...bool) (*Factory, Profile, *atomic.Int32) {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { l.Close() })
	host, ps, _ := net.SplitHostPort(l.Addr().String())
	port, _ := strconv.Atoi(ps)
	count := new(atomic.Int32)
	go func() {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			go func() {
				defer c.Close()
				var mechanism, clientFirst, serverFirst string
				for {
					var sz [4]byte
					if _, e := io.ReadFull(c, sz[:]); e != nil {
						return
					}
					b := make([]byte, binary.BigEndian.Uint32(sz[:]))
					if _, e := io.ReadFull(c, b); e != nil {
						return
					}
					key := int16(binary.BigEndian.Uint16(b))
					version := int16(binary.BigEndian.Uint16(b[2:]))
					var r kmsg.Response
					switch key {
					case 18:
						v := kmsg.NewPtrApiVersionsResponse()
						v.ApiKeys = []kmsg.ApiVersionsResponseApiKey{{ApiKey: 18, MaxVersion: 3}, {ApiKey: 17, MaxVersion: 1}, {ApiKey: 36, MaxVersion: 1}, {ApiKey: 3, MaxVersion: 9}, {ApiKey: 50, MaxVersion: 0}, {ApiKey: 51, MaxVersion: 0}, {ApiKey: 32, MaxVersion: 2}, {ApiKey: 37, MaxVersion: 1}, {ApiKey: 44, MaxVersion: 0}, {ApiKey: 29, MaxVersion: 1}, {ApiKey: 31, MaxVersion: 1}}
						r = v
					case 17:
						if len(saslMode) == 0 || !saslMode[0] {
							return
						}
						q := kmsg.NewPtrSASLHandshakeRequest()
						q.SetVersion(version)
						_ = q.ReadFrom(b[10+int(binary.BigEndian.Uint16(b[8:])):])
						mechanism = q.Mechanism
						v := kmsg.NewPtrSASLHandshakeResponse()
						v.SupportedMechanisms = []string{"PLAIN", "SCRAM-SHA-256"}
						r = v
					case 36:
						q := kmsg.NewPtrSASLAuthenticateRequest()
						q.SetVersion(version)
						_ = q.ReadFrom(b[10+int(binary.BigEndian.Uint16(b[8:])):])
						v := kmsg.NewPtrSASLAuthenticateResponse()
						if mechanism == "SCRAM-SHA-256" {
							if clientFirst == "" {
								clientFirst = strings.TrimPrefix(string(q.SASLAuthBytes), "n,,")
								nonce := strings.Split(clientFirst, ",r=")[1]
								serverFirst = "r=" + nonce + "syntheticserver,s=" + base64.StdEncoding.EncodeToString([]byte("synthetic-salt")) + ",i=8192"
								v.SASLAuthBytes = []byte(serverFirst)
							} else {
								final := string(q.SASLAuthBytes)
								idx := strings.LastIndex(final, ",p=")
								if idx < 0 {
									return
								}
								auth := clientFirst + "," + serverFirst + "," + final[:idx]
								salted := pbkdf2.Key([]byte("synthetic-password"), []byte("synthetic-salt"), 8192, 32, sha256.New)
								mac := func(key []byte, value string) []byte {
									h := hmac.New(sha256.New, key)
									h.Write([]byte(value))
									return h.Sum(nil)
								}
								clientKey := mac(salted, "Client Key")
								stored := sha256.Sum256(clientKey)
								sig := mac(stored[:], auth)
								for i := range clientKey {
									clientKey[i] ^= sig[i]
								}
								proof, _ := base64.StdEncoding.DecodeString(final[idx+3:])
								if !hmac.Equal(proof, clientKey) {
									v.ErrorCode = 58
								} else {
									v.SASLAuthBytes = []byte("v=" + base64.StdEncoding.EncodeToString(mac(mac(salted, "Server Key"), auth)))
								}
							}
						} else if string(q.SASLAuthBytes) != "\x00synthetic-admin\x00synthetic-admin-password" {
							v.ErrorCode = 58
						}
						r = v
					case 3:
						v := kmsg.NewPtrMetadataResponse()
						if mechanism == "SCRAM-SHA-256" {
							v.ErrorCode = 31
						}
						v.ControllerID = 1
						v.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 1, Host: host, Port: int32(port)}}
						r = v
					case 50:
						v := kmsg.NewPtrDescribeUserSCRAMCredentialsResponse()
						v.Results = []kmsg.DescribeUserSCRAMCredentialsResponseResult{{User: "synthetic", ErrorCode: code, CredentialInfos: []kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo{{Mechanism: 1, Iterations: 8192}}}}
						r = v
					case 51:
						q := kmsg.NewPtrAlterUserSCRAMCredentialsRequest()
						q.SetVersion(version)
						offset := 10 + int(binary.BigEndian.Uint16(b[8:])) + 1
						if e := q.ReadFrom(b[offset:]); e != nil || len(q.Upsertions) != 1 || len(q.Deletions) != 0 {
							t.Error("invalid SCRAM mutation", e)
							return
						}
						u := q.Upsertions[0]
						if u.Name != "synthetic" || u.Mechanism != 1 || u.Iterations != 8192 || len(u.Salt) != 32 || !bytes.Equal(u.SaltedPassword, pbkdf2.Key([]byte("synthetic-password"), u.Salt, 8192, 32, sha256.New)) {
							t.Error("invalid salted credential")
						}
						count.Add(1)
						v := kmsg.NewPtrAlterUserSCRAMCredentialsResponse()
						v.Results = []kmsg.AlterUserSCRAMCredentialsResponseResult{{User: "synthetic"}}
						r = v
					default:
						return
					}
					r.SetVersion(version)
					out := append([]byte{}, b[4:8]...)
					if r.IsFlexible() && key != 18 {
						out = append(out, 0)
					}
					out = r.AppendTo(out)
					binary.BigEndian.PutUint32(sz[:], uint32(len(out)))
					if _, e := c.Write(append(sz[:], out...)); e != nil {
						return
					}
				}
			}()
		}
	}()
	policy, _ := NewNetworkPolicy(NetworkPolicyConfig{AllowedCIDRs: []string{"127.0.0.0/8"}, AllowedPorts: []int{port}})
	f, _ := NewFactory(policy, nil)
	if len(saslMode) > 0 && saslMode[0] {
		f.secrets = memorySecrets{"u": []byte("synthetic-admin"), "p": []byte("synthetic-admin-password")}
		return f, Profile{BootstrapServers: []string{l.Addr().String()}, SecurityProtocol: "SASL_PLAINTEXT", SASLMechanism: "PLAIN", UsernameSecretRef: "u", PasswordSecretRef: "p"}, count
	}
	return f, Profile{BootstrapServers: []string{l.Addr().String()}, SecurityProtocol: "PLAINTEXT"}, count
}

func TestSCRAMNativeCreatePreflight(t *testing.T) {
	for _, code := range []int16{0, 91, 31} {
		f, p, count := scramBroker(t, code)
		err := f.CreateSCRAM(context.Background(), p, "synthetic", "synthetic-password")
		if code == 91 {
			if err != nil || count.Load() != 1 {
				t.Fatal(err, count.Load())
			}
		} else {
			var safe *kafkaops.Error
			if !errors.As(err, &safe) || count.Load() != 0 {
				t.Fatal(err, count.Load())
			}
			if code == 0 && safe.Code != "CONFLICT" {
				t.Fatal(err)
			}
		}
	}
}
func TestSCRAMNativeListSafeMetadata(t *testing.T) {
	f, p, _ := scramBroker(t, 0)
	users, err := f.ListSCRAM(context.Background(), p)
	if err != nil || len(users) != 1 || users[0].Username != "synthetic" || users[0].Mechanism != "SCRAM-SHA-256" || users[0].Iterations != 8192 {
		t.Fatal(users, err)
	}
	b, _ := json.Marshal(users)
	if strings.Contains(string(b), "password") {
		t.Fatal("credential leaked")
	}
}
func TestSCRAMVerifyRequiresCompletedSASL(t *testing.T) {
	f, p, _ := scramBroker(t, 0)
	f.secrets = memorySecrets{"u": []byte("synthetic-admin"), "p": []byte("synthetic-admin-password")}
	p.SecurityProtocol = "SASL_PLAINTEXT"
	p.SASLMechanism = "PLAIN"
	p.UsernameSecretRef = "u"
	p.PasswordSecretRef = "p"
	// This broker advertises no SASL support: its successful pre-authentication
	// ApiVersions response must never be accepted as credential verification.
	if err := f.VerifySCRAM(context.Background(), p, "synthetic", "synthetic-password"); err == nil {
		t.Fatal("pre-authentication ApiVersions accepted")
	}
}
func TestSCRAMVerifyBusinessAuthenticationWithoutMetadataACL(t *testing.T) {
	f, p, _ := scramBroker(t, 0, true)
	if err := f.VerifySCRAM(context.Background(), p, "synthetic", "synthetic-password"); err != nil {
		t.Fatal(err)
	}
	err := f.VerifySCRAM(context.Background(), p, "synthetic", "wrong-synthetic-password")
	var safe *kafkaops.Error
	if !errors.As(err, &safe) || safe.Code != "AUTHENTICATION_FAILED" {
		t.Fatal("wrong password was not rejected safely", err)
	}
}
func TestSCRAMSeparateListenerUsesNoManagementSecrets(t *testing.T) {
	f, management, _ := scramBroker(t, 0)
	listenerFactory, listener, _ := scramBroker(t, 0, true)
	for port := range listenerFactory.policy.ports {
		f.policy.ports[port] = true
	}
	listener.UsernameSecretRef = "absent"
	listener.PasswordSecretRef = "absent"
	if err := f.VerifySCRAMListener(context.Background(), management, listener, "synthetic", "synthetic-password"); err != nil {
		t.Fatal(err)
	}
	listener.BootstrapServers = []string{"127.0.0.1:1"}
	err := f.VerifySCRAMListener(context.Background(), management, listener, "synthetic", "synthetic-password")
	var safe *kafkaops.Error
	if !errors.As(err, &safe) || safe.Code != "NETWORK_DENIED" {
		t.Fatal("listener bypassed network policy", err)
	}
}
