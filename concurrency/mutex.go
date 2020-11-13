package concurrency

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/MrCroxx/etcduck/session"
	v3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
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
	Resource() string
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
func NewMutex(client *v3.Client, resource string) (Mutex, error) {
	s, err := session.NewSession(client, session.WithTTL(3))
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
		// get
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
		log.Printf("key:%s rev:%d", m.key, r.Header.Revision)
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

// func (m *mutex) waitMutex(ctx context.Context, maxCreateRev int64) error {
// 	c := m.s.Client()

// 	getOpts := append(v3.WithLastCreate(), v3.WithMaxCreateRev(maxCreateRev))
// 	for {
// 		r, err := c.Get(context.TODO(), m.res, getOpts...)
// 		if err != nil {
// 			return err
// 		}
// 		if len(r.Kvs) == 0 {
// 			return nil
// 		}
// 		latest := string(r.Kvs[0].Key)
// 		if err = m.waitMutexDelete(ctx, latest, r.Header.Revision); err != nil {
// 			return err
// 		}
// 	}
// }

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
		case err := <-waitError(func() error { return m.waitMutexDelete(cctx, latest, r.Header.Revision) }):
			// receive error, return
			if err != nil {
				return err
			}
			// deleted, watch the next latest key
		}
	}
}

// func (m *mutex) waitMutexDelete(ctx context.Context, key string, rev int64) error {
// 	cctx, cancel := context.WithCancel(ctx)
// 	defer cancel()

// 	c := m.s.Client()
// 	var wr v3.WatchResponse
// 	w := c.Watch(context.TODO(), key, v3.WithRev(rev))
// 	for wr = range w {
// 		for _, ev := range wr.Events {
// 			if ev.Type == mvccpb.DELETE {
// 				return nil
// 			}
// 		}
// 	}
// 	if err := wr.Err(); err != nil {
// 		return err
// 	}
// 	if err := ctx.Err(); err != nil {
// 		return err
// 	}
// 	return fmt.Errorf("lost watcher waiting for delete")
// }

func (m *mutex) waitMutexDelete(ctx context.Context, key string, rev int64) error {
	log.Printf("wait delete key:%s rev:%d", key, rev)
	w := m.s.Client().Watch(context.TODO(), key, v3.WithRev(rev))
	// func returns in loop
	for {
		select {
		// receive done signal, return
		case <-ctx.Done():
			return ctx.Err()
		// receive watch response
		case wr, ok := <-w:
			// if not ok, watch channel has been closed unexpectedly, return error
			if !ok {
				if err := wr.Err(); err != nil {
					return err
				}
				return fmt.Errorf("lost watcher waiting for delete")
			}
			for _, ev := range wr.Events {
				// receive DELETE signal, return
				if ev.Type == mvccpb.DELETE {
					return nil
				}
			}
			// not receive DELETE signal, continue
		}
	}
}

// func (m *mutex) waitMutexC(ctx context.Context, maxCreateRev int64, done <-chan struct{}) <-chan error {
// 	ch := make(chan error)

// 	go func() {
// 		getOpts := append(v3.WithLastCreate(), v3.WithMaxCreateRev(maxCreateRev))
// 		for {
// 			// get the latest key to watch
// 			r, err := m.s.Client().Get(context.TODO(), m.res, getOpts...)
// 			if err != nil {
// 				ch <- err
// 				return
// 			}
// 			// no key to wait
// 			if len(r.Kvs) == 0 {
// 				ch <- nil
// 				return
// 			}
// 			// watch the latest key
// 			latest := string(r.Kvs[0].Key)
// 			select {
// 			// receive done signal, return
// 			case <-done:
// 				return
// 			case err
// 			}
// 		}

// 	}()

// 	return ch
// }

// func (m *mutex) waitMutexDelete(ctx context.Context, key string, rev int64, done <-chan struct{}) <-chan error {
// 	cctx, cancel := context.WithCancel(ctx)
// 	ch := make(chan error)
// 	// watch key delete and done signal
// 	go func() {
// 		w := m.s.Client().Watch(cctx, key, v3.WithRev(rev))
// 		defer cancel()
// 		for {
// 			select {
// 			// receive done signal, return
// 			case <-done:
// 				return
// 			case wr, ok := <-w:
// 				if ok {
// 					for _, ent := range wr.Events {
// 						if ent.Type == mvccpb.DELETE {
// 							// receive delete, return and write ch
// 							ch <- nil
// 							return
// 						}
// 					}
// 				}
// 				// if not ok, watch channel is closed unexpectedly
// 				if err := wr.Err(); err != nil {
// 					ch <- err
// 					return
// 				}
// 				if err := ctx.Err(); err != nil {
// 					ch <- err
// 					return
// 				}
// 				ch <- fmt.Errorf("lost watcher waiting for delete")
// 				return
// 			}
// 		}
// 	}()
// 	return ch
// }
