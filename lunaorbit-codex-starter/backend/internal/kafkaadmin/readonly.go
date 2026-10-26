package kafkaadmin

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// readOnlyKeys is the approved read set plus the two SASL frames needed to authenticate.
// Anything else is refused inside the connection before a single byte is written.
var readOnlyKeys = map[int16]bool{
	int16(kmsg.Metadata): true, int16(kmsg.ApiVersions): true, int16(kmsg.DescribeCluster): true,
	int16(kmsg.DescribeConfigs): true, int16(kmsg.ListOffsets): true, int16(kmsg.OffsetFetch): true,
	int16(kmsg.DescribeGroups): true, int16(kmsg.ListGroups): true, int16(kmsg.FindCoordinator): true,
	int16(kmsg.DescribeLogDirs): true,
	int16(kmsg.SASLHandshake):   true, int16(kmsg.SASLAuthenticate): true,
}

// ErrWriteRefused is returned when a request type outside the read-only allowlist is attempted.
var ErrWriteRefused = errors.New("kafka request refused by read-only allowlist")

// readOnlyConn inspects each outgoing Kafka request frame (4-byte size, then int16 api key)
// and refuses non-allowlisted keys without writing the frame. It sits above TLS, so it sees
// plaintext frames.
type readOnlyConn struct {
	net.Conn
	mu      sync.Mutex
	header  []byte // partial header of the next frame
	remain  int    // bytes left of the current, already-approved frame
	refused bool
}

func (c *readOnlyConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refused {
		return 0, ErrWriteRefused
	}
	written := 0
	for len(p) > 0 {
		if c.remain > 0 {
			n := min(c.remain, len(p))
			m, err := c.Conn.Write(p[:n])
			written += m
			c.remain -= m
			if err != nil {
				return written, err
			}
			p = p[n:]
			continue
		}
		need := 6 - len(c.header)
		if len(p) < need {
			c.header = append(c.header, p...)
			return written + len(p), nil
		}
		frame := append(append([]byte{}, c.header...), p[:need]...)
		key := int16(binary.BigEndian.Uint16(frame[4:6]))
		size := int(binary.BigEndian.Uint32(frame[:4]))
		if !readOnlyKeys[key] || size < 2 {
			// Poison the connection: a partially framed stream must never continue.
			c.refused = true
			return written, ErrWriteRefused
		}
		m, err := c.Conn.Write(frame)
		if err != nil {
			return written, err
		}
		written += need
		_ = m
		c.header = c.header[:0]
		c.remain = size - 2
		p = p[need:]
	}
	return written, nil
}

// Dialer opens a TCP connection; NetworkPolicy-aware callers pass a dialer that enforces it.
type Dialer func(ctx context.Context, network, address string) (net.Conn, error)

// NewReadOnlyClient builds a franz-go client whose connections only ever carry
// allowlisted request types. mechanism may be nil (no SASL); tlsCfg may be nil.
func NewReadOnlyClient(seeds []string, mechanism sasl.Mechanism, tlsCfg *tls.Config, dial Dialer) (*kgo.Client, error) {
	if len(seeds) == 0 || dial == nil {
		return nil, errors.New("read-only client needs seeds and a dialer")
	}
	guarded := func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if tlsCfg != nil {
			cfg := tlsCfg.Clone()
			if cfg.ServerName == "" {
				cfg.ServerName, _, _ = net.SplitHostPort(address)
			}
			secure := tls.Client(conn, cfg)
			if err := secure.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, safeError("tls_failed")
			}
			conn = secure
		}
		return &readOnlyConn{Conn: conn}, nil
	}
	opts := []kgo.Opt{kgo.SeedBrokers(seeds...), kgo.Dialer(guarded), kgo.DialTimeout(5 * time.Second), kgo.RequestRetries(0), kgo.RetryTimeout(5 * time.Second), kgo.MetadataMaxAge(time.Minute), kgo.ProducerLinger(0), kgo.DisableIdempotentWrite()}
	if mechanism != nil {
		opts = append(opts, kgo.SASL(safeMechanism{mechanism}))
	}
	return kgo.NewClient(opts...)
}

func scramMechanism(user, pass string) sasl.Mechanism {
	return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()
}
