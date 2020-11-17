package concurrency

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MrCroxx/etcduck/session"
	v3 "go.etcd.io/etcd/clientv3"
)

// RWLock is a distributed read-write lock.
type RWLock interface {
	// RLock acquires a distributed read lock.
	// If timeout is set to be 0, the method will be blocked until acquiring succeeds.
	RLock(ctx context.Context, timeout time.Duration) (*RWLockInfo, error)
	// Lock acquires a distributed write lock.
	// If timeout is set to be 0, the method will be blocked until acquiring succeeds.
	// Note that RWLock.Lock is not reentrant.
	Lock(ctx context.Context, timeout time.Duration) (*RWLockInfo, error)
	// Unlock releases the distribited lock.
	// If the distributed lock does not exist, return error.
	Unlock(ctx context.Context) error
	// // Resource returns the resource held by RWLock.
	// Resource() string
	// // Key returns the read key of RWLock.
	// // Returns empty string before locking.
	// RKey() string
	// // Key returns the write key of RWLock.
	// // Returns empty string before locking.
	// WKey() string
}

// RWLockInfo is the information of the acquired RWLock.
type RWLockInfo struct {
	// LeaseID is the lease id of the session held by RWLock.
	LeaseID v3.LeaseID
	// Resource is the resource held by RWLock.
	Resource string
	// Key is the read key of RWLock.
	RKey string
	// Key is the write key of RWLock.
	WKey string
	// Done will close while the session held by RWLock finishes or breaks.
	Done <-chan struct{}
}

type rwlock struct {
	s      session.Session
	opts   *options
	client *v3.Client
	res    string
	rres   string
	wres   string
	rkey   string
	wkey   string
	done   <-chan struct{}
	mu     sync.Mutex
}

// NewRWLock returns a new RWLock on the resource.
// Note that RWLock will not create a new session with lease,
// the leased session will be created when locking,
// the key based on both leaseID and resource.
func NewRWLock(client *v3.Client, resource string, options ...Option) (RWLock, error) {
	opts := defaultOptions()
	for _, option := range options {
		option(opts)
	}

	res := strings.Join([]string{prefix, "rwlock", resource}, ":")
	rres := strings.Join([]string{res, "r"}, ":")
	wres := strings.Join([]string{res, "w"}, ":")

	return &rwlock{s: nil, opts: opts, client: client, res: res, rres: rres, wres: wres, rkey: "", wkey: ""}, nil
}

func (rw *rwlock) RLock(ctx context.Context, timeout time.Duration) (*RWLockInfo, error) {
	err := rw.acquireSessionAndKey()
	if err != nil {
		return nil, err
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if timeout.Nanoseconds() == 0 {
		timeout = defaultTimeout
	}
	select {
	case <-time.After(timeout):
		return nil, timeoutError(timeout)
	case err := <-waitError(func() error {
		// return true if rkey doesn't exist
		cmp := v3.Compare(v3.CreateRevision(rw.rkey), "=", 0)
		// put rkey to etcd with lease
		put := v3.OpPut(rw.rkey, "", v3.WithLease(rw.s.Lease()))
		// get the existed rkey
		get := v3.OpGet(rw.rkey)
		// commit txn
		r, err := rw.s.Client().Txn(cctx).
			If(cmp).
			Then(put).
			Else(get).
			Commit()
		if err != nil || r == nil {
			return err
		}
		var rev int64
		if r.Succeeded {
			rev = r.Header.Revision - 1
		} else {
			rev = r.Responses[0].GetResponseRange().Kvs[0].CreateRevision - 1
		}
		// wait for the wkeys before this to be deleted
		return rw.waitWKeysDelete(cctx, rev)
	}):
		return &RWLockInfo{
			LeaseID:  rw.s.Lease(),
			Resource: rw.res,
			RKey:     rw.rkey,
			WKey:     rw.wkey,
			Done:     rw.done,
		}, err
	}
}

func (rw *rwlock) Lock(ctx context.Context, timeout time.Duration) (*RWLockInfo, error) {
	err := rw.acquireSessionAndKey()
	if err != nil {
		return nil, err
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if timeout.Nanoseconds() == 0 {
		timeout = defaultTimeout
	}
	select {
	case <-time.After(timeout):
		return nil, timeoutError(timeout)
	case err := <-waitError(func() error {
		// return true if rkey doesn't exist
		cmp := v3.Compare(v3.CreateRevision(rw.wkey), "=", 0)
		// put rkey to etcd with lease
		put := v3.OpPut(rw.wkey, "", v3.WithLease(rw.s.Lease()))
		// commit txn
		r, err := rw.s.Client().Txn(cctx).
			If(cmp).
			Then(put).
			Commit()
		if err != nil || r == nil {
			return err
		}
		if !r.Succeeded {
			return fmt.Errorf("lock function in rwlock is not reentrant")
		}
		// wait for the wkeys before this to be deleted
		return rw.waitRWKeysDelete(cctx, r.Header.Revision-1)
	}):
		return &RWLockInfo{
			LeaseID:  rw.s.Lease(),
			Resource: rw.res,
			RKey:     rw.rkey,
			WKey:     rw.wkey,
			Done:     rw.done,
		}, err
	}
}

func (rw *rwlock) Unlock(ctx context.Context) error {
	defer rw.releaseSessionAndKey()
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if rw.s == nil {
		return fmt.Errorf("rwlock is not holding a leased session, may be not locked yet")
	}
	if _, err := rw.s.Client().Delete(cctx, rw.rkey); err != nil {
		return err
	}
	if _, err := rw.s.Client().Delete(cctx, rw.wkey); err != nil {
		return err
	}
	return nil
}

func (rw *rwlock) acquireSessionAndKey() error {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if rw.s != nil {
		return nil
	}
	sessionOpts := append([]session.Option{},
		session.WithTTL(rw.opts.leaseTimeout), session.WithLease(rw.opts.leaseID),
	)
	s, err := session.NewSession(rw.client, sessionOpts...)
	if err != nil || s == nil {
		return err
	}
	rw.s = s
	rw.done = s.Done()
	rw.rkey = strings.Join([]string{rw.rres, strconv.FormatInt(int64(s.Lease()), 10)}, ":")
	rw.wkey = strings.Join([]string{rw.wres, strconv.FormatInt(int64(s.Lease()), 10)}, ":")
	return nil
}

func (rw *rwlock) releaseSessionAndKey() error {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if rw.s == nil {
		return nil
	}
	err := rw.s.Close()
	rw.s = nil
	rw.rkey = ""
	rw.wkey = ""
	return err
}

func (rw *rwlock) waitWKeysDelete(ctx context.Context, maxCreateRev int64) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	getOpts := append(v3.WithLastCreate(), v3.WithMaxCreateRev(maxCreateRev))
	for {
		r, err := rw.s.Client().Get(cctx, rw.wres, getOpts...)
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
		case err := <-waitError(func() error { return waitKeyDelete(cctx, rw.s.Client(), latest, r.Header.Revision) }):
			// receive error, return
			if err != nil {
				return err
			}
			// deleted, watch the next latest key
		}
	}
}

func (rw *rwlock) waitRWKeysDelete(ctx context.Context, maxCreateRev int64) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	getOpts := append(v3.WithLastCreate(), v3.WithMaxCreateRev(maxCreateRev))
	for {
		r, err := rw.s.Client().Get(cctx, rw.res, getOpts...)
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
		case err := <-waitError(func() error { return waitKeyDelete(cctx, rw.s.Client(), latest, r.Header.Revision) }):
			// receive error, return
			if err != nil {
				return err
			}
			// deleted, watch the next latest key
		}
	}
}
