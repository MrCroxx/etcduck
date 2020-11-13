package session

import (
	"context"
	"time"

	v3 "go.etcd.io/etcd/clientv3"
)

const (
	defaultTTL = 60
)

// Session represents a lease kept alive for the lifetime of a client.
// Fault-tolerant applications may use sessions to reason about liveness.
type Session interface {
	// Client is the etcd client that is attached to the session.
	Client() *v3.Client
	// Lease is the lease ID for keys bound to the session.
	Lease() v3.LeaseID
	// Done returns a channel that closes when the lease is orphaned, expires, or
	// is otherwise no longer being refreshed.
	Done() <-chan struct{}
	// Orphan ends the refresh for the session lease. This is useful
	// in case the state of the client connection is indeterminate (revoke
	// would fail) or when transferring lease ownership.
	Orphan()
	// Close orphans the session and revokes the session lease.
	Close() error
}

type session struct {
	client *v3.Client
	id     v3.LeaseID
	opts   *options

	cancel context.CancelFunc
	donec  <-chan struct{}
}

// NewSession gets the leased session for a client.
func NewSession(client *v3.Client, opts ...Option) (Session, error) {
	ops := &options{ttl: defaultTTL, ctx: client.Ctx()}
	for _, opt := range opts {
		opt(ops)
	}
	id := ops.leaseID
	if id == v3.NoLease {
		r, err := client.Grant(ops.ctx, ops.ttl)
		if err != nil {
			return nil, err
		}
		id = r.ID
	}

	ctx, cancel := context.WithCancel(ops.ctx)
	ka, err := client.KeepAlive(ctx, id)
	if err != nil || ka == nil {
		cancel()
		return nil, err
	}

	donec := make(chan struct{})

	go func() {
		defer close(donec)
		for range ka {
			// drop messages until keep alive channel closes, see https://pkg.go.dev/go.etcd.io/etcd/clientv3#Lease.
		}
	}()

	return &session{client: client, opts: ops, id: id, cancel: cancel, donec: donec}, nil
}

func (s *session) Client() *v3.Client {
	return s.client
}

func (s *session) Lease() v3.LeaseID { return s.id }

func (s *session) Done() <-chan struct{} { return s.donec }

func (s *session) Orphan() {
	s.cancel()
	<-s.donec
}

func (s *session) Close() error {
	s.Orphan()
	// if revoke takes longer than the ttl, lease is expired anyway
	ctx, cancel := context.WithTimeout(s.opts.ctx, time.Duration(s.opts.ttl)*time.Second)
	_, err := s.client.Revoke(ctx, s.id)
	cancel()
	return err
}

// options

type options struct {
	ttl     int64
	leaseID v3.LeaseID
	ctx     context.Context
}

// Option for configuring session.
type Option func(*options)

// WithTTL configures the session's TTL in seconds.
// If ttl <= 0, the default 60 seconds TTL will be used.
func WithTTL(ttl int64) Option {
	return func(opts *options) {
		if ttl > 0 {
			opts.ttl = ttl
		} else {
			opts.ttl = defaultTTL
		}
	}
}

// WithLease specifies the existing leaseID to be used for the session.
// This is useful in process restart scenario, for example, to reclaim
// leadership from an election prior to restart.
func WithLease(leaseID v3.LeaseID) Option {
	return func(opts *options) {
		opts.leaseID = leaseID
	}
}

// WithContext assigns a context to the session instead of defaulting to
// using the client context. This is useful for canceling NewSession and
// Close operations immediately without having to close the client. If the
// context is canceled before Close() completes, the session's lease will be
// abandoned and left to expire instead of being revoked.
func WithContext(ctx context.Context) Option {
	return func(opts *options) {
		opts.ctx = ctx
	}
}
