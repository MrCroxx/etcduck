package concurrency

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/MrCroxx/etcduck/session"
	v3 "go.etcd.io/etcd/clientv3"
)

// Mutex is a distributed mutex lock.
type Mutex interface {
	// Lock acquires a distributed lock.
	// If timeout is set to be 0, the method will be blocked until acquiring succeeds.
	// Note that Mutex is not reentrant.
	Lock(ctx context.Context, timeout time.Duration) error
	// Unlock releases the distribited lock.
	// If the distributed lock does not exist, return error.
	Unlock(ctx context.Context) error
	// Resource returns the resource held by Mutex.
	Resource() string
	// Key returns the key of Mutex.
	Key() string
}

type mutex struct {
	s   session.Session
	res string
	key string
}

// NewMutex returns a new Mutex on the resource.
// Note that NewMutex will create a new session with lease,
// the key based on both leaseID and resource.
func NewMutex(client *v3.Client, resource string, options ...Option) (Mutex, error) {
	opts := defaultOptions()
	for _, option := range options {
		option(opts)
	}
	s, err := session.NewSession(client, session.WithTTL(opts.leaseTimeout))
	if err != nil || s == nil {
		return nil, err
	}
	res := strings.Join([]string{prefix, "mutex", resource}, ":")
	key := strings.Join([]string{res, strconv.FormatInt(int64(s.Lease()), 10)}, ":")
	return &mutex{s: s, res: res, key: key}, nil
}

func (m *mutex) Lock(ctx context.Context, timeout time.Duration) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if timeout.Nanoseconds() == 0 {
		timeout = defaultTimeout
	}
	select {
	case <-time.After(timeout):
		return timeoutError(timeout)
	case err := <-waitError(func() error {
		// return true if key doesn't exist
		cmp := v3.Compare(v3.CreateRevision(m.key), "=", 0)
		// put key to etcd with lease
		put := v3.OpPut(m.key, "", v3.WithLease(m.s.Lease()))
		// commit txn
		r, err := m.s.Client().Txn(cctx).
			If(cmp).
			Then(put).
			Commit()
		if err != nil || r == nil {
			return err
		}
		if !r.Succeeded {
			return fmt.Errorf("mutex is not reentrant")
		}
		// wait for the keys before this to be deleted
		return m.waitMutex(cctx, r.Header.Revision-1)
	}):
		return err
	}
}

func (m *mutex) Unlock(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if _, err := m.s.Client().Delete(cctx, m.key); err != nil {
		return err
	}
	return nil
}

func (m *mutex) Resource() string {
	return m.res
}

func (m *mutex) Key() string {
	return m.key
}

func (m *mutex) waitMutex(ctx context.Context, maxCreateRev int64) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	getOpts := append(v3.WithLastCreate(), v3.WithMaxCreateRev(maxCreateRev))
	for {
		r, err := m.s.Client().Get(cctx, m.res, getOpts...)
		if err != nil {
			return err
		}
		if len(r.Kvs) == 0 {
			return nil
		}
		latest := string(r.Kvs[0].Key)
		select {
		// receive done signal, return
		case <-ctx.Done():
			return ctx.Err()
		case err := <-waitError(func() error { return waitKeyDelete(cctx, m.s.Client(), latest, r.Header.Revision) }):
			// receive error, return
			if err != nil {
				return err
			}
			// deleted, watch the next latest key
		}
	}
}
