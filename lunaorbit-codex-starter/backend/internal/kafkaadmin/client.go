package kafkaadmin

import (
	"context"
	"crypto/tls"
	"github.com/twmb/franz-go/pkg/kgo"
	"net"
	"time"
)

func (f *Factory) clientOptions(ctx context.Context, p Profile) ([]kgo.Opt, error) {
	tc, mechanism, e := f.security(ctx, p)
	if e != nil {
		return nil, e
	}
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		ips, e := f.policy.resolve(ctx, address)
		if e != nil {
			return nil, e
		}
		conn, e := (&net.Dialer{Timeout: f.policy.timeout}).DialContext(ctx, "tcp", ips[0])
		if e != nil {
			return nil, classify(e)
		}
		if tc != nil {
			cfg := tc.Clone()
			if cfg.ServerName == "" {
				cfg.ServerName, _, _ = net.SplitHostPort(address)
			}
			secure := tls.Client(conn, cfg)
			if e := secure.HandshakeContext(ctx); e != nil {
				conn.Close()
				return nil, safeError("tls_failed")
			}
			return secure, nil
		}
		return conn, nil
	}
	opts := []kgo.Opt{kgo.SeedBrokers(p.BootstrapServers...), kgo.Dialer(dial), kgo.DialTimeout(f.policy.timeout), kgo.RequestTimeoutOverhead(f.policy.timeout), kgo.RequestRetries(0), kgo.RetryTimeout(f.policy.timeout), kgo.MetadataMaxAge(time.Minute)}
	if mechanism != nil {
		opts = append(opts, kgo.SASL(safeMechanism{mechanism}))
	}
	return opts, nil
}
