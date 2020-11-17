package concurrency

import (
	v3 "go.etcd.io/etcd/clientv3"
)

// Option to configure concurrency objects
type Option func(*options)

type options struct {
	leaseTimeout int64
	leaseID      v3.LeaseID
}

func defaultOptions() *options {
	return &options{
		leaseTimeout: 3,
		leaseID:      v3.NoLease,
	}
}

// WithLeaseTimeout specify lease timeout in seconds for leased session.
func WithLeaseTimeout(timeout int64) Option {
	return func(opts *options) { opts.leaseTimeout = timeout }
}

// WithLeaseID specify lease id for leased session.
func WithLeaseID(leaseID v3.LeaseID) Option {
	return func(opts *options) { opts.leaseID = leaseID }
}
