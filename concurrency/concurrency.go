package concurrency

import (
	"fmt"
	"time"

	v3 "go.etcd.io/etcd/clientv3"
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
