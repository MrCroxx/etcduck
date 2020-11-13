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

type RWLock interface {
	RLock(ctx context.Context, timeout time.Duration) error
	Lock(ctx context.Context, timeout time.Duration) error
	Unlock(ctx context.Context) error
	Resource() string
	RKey() string
	WKey() string
}

type rwlock struct {
	s    session.Session
	res  string
	rres string
	wres string
	rkey string
	wkey string
}

func NewRWLock(client *v3.Client, resource string, options ...Option) (RWLock, error) {
	opts := defaultOptions()
	for _, option := range options {
		option(opts)
	}
	s, err := session.NewSession(client, session.WithTTL(opts.leaseTimeout))
	if err != nil || s == nil {
		return nil, err
	}
	res := strings.Join([]string{prefix, "rwlock", resource}, ":")
	rres := strings.Join([]string{res, "r"}, ":")
	wres := strings.Join([]string{res, "w"}, ":")

	rkey := strings.Join([]string{rres, strconv.FormatInt(int64(s.Lease()), 10)}, ":")
	wkey := strings.Join([]string{wres, strconv.FormatInt(int64(s.Lease()), 10)}, ":")
	return &rwlock{s: s, res: res, rres: rres, wres: wres, rkey: rkey, wkey: wkey}, nil
}

func (rw *rwlock) RLock(ctx context.Context, timeout time.Duration) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if timeout.Nanoseconds() == 0 {
		timeout = defaultTimeout
	}
	select {
	case <-time.After(timeout):
		return timeoutError(timeout)
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
		return err
	}
}

func (rw *rwlock) Lock(ctx context.Context, timeout time.Duration) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if timeout.Nanoseconds() == 0 {
		timeout = defaultTimeout
	}
	select {
	case <-time.After(timeout):
		return timeoutError(timeout)
	case err := <-waitError(func() error {
		// return true if rkey doesn't exist
		cmp := v3.Compare(v3.CreateRevision(rw.wkey), "=", 0)
		// put rkey to etcd with lease
		put := v3.OpPut(rw.rkey, "", v3.WithLease(rw.s.Lease()))
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
		return err
	}
}

func (rw *rwlock) Unlock(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if _, err := rw.s.Client().Delete(cctx, rw.rkey); err != nil {
		return err
	}
	if _, err := rw.s.Client().Delete(cctx, rw.wkey); err != nil {
		return err
	}
	return nil
}

func (rw *rwlock) Resource() string {
	return rw.res
}

func (rw *rwlock) RKey() string {
	return rw.rkey
}
func (rw *rwlock) WKey() string {
	return rw.wkey
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
