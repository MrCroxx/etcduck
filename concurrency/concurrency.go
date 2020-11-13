package concurrency

import (
	"context"
	"fmt"
	"time"

	v3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

// default

const (
	prefix         = "etcduck"
	defaultTimeout = time.Minute * 5
)

// txn

type commitr struct {
	r *v3.TxnResponse
	e error
}

func waitError(f func() error) <-chan error {
	ch := make(chan error)
	go func() {
		ch <- f()
	}()
	return ch
}

func timeoutError(d time.Duration) error {
	return fmt.Errorf("timeout (%s)", d.String())
}

func waitKeyDelete(ctx context.Context, client *v3.Client, key string, rev int64) error {
	w := client.Watch(context.TODO(), key, v3.WithRev(rev))
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
