package concurrency

// Option to configure concurrency objects
type Option func(*options)

type options struct {
	leaseTimeout int64
}

func defaultOptions() *options {
	return &options{
		leaseTimeout: 3,
	}
}

// WithLeaseTimeout specify lease timeout for etcd lease connection in seconds.
func WithLeaseTimeout(timeout int64) Option {
	return func(opts *options) { opts.leaseTimeout = timeout }
}
