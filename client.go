package etcdance

import (
	"fmt"

	"github.com/MrCroxx/etcdance/snowflake"
	"go.etcd.io/etcd/clientv3"
)

// Client : etcdancer.Client
type Client interface {
	ID() int64
}

type client struct {
	id  int64
	cli *clientv3.Client
	kvc clientv3.KV
}

// New : Create etcdance.Client from *etcd.clientv3.Client
func New(cli *clientv3.Client, datacenterID int64, workerID int64) (Client, error) {
	// check args
	if cli == nil {
		return nil, fmt.Errorf("etcd client cannot be nil")
	}

	// generate client id
	snowflake.InitOrReset(datacenterID, workerID)
	id, err := snowflake.NextID()
	if err != nil || id == -1 {
		return nil, err
	}

	// return client
	return &client{
		id:  id,
		cli: cli,
		kvc: clientv3.NewKV(cli),
	}, nil
}

// Implement

// ID : returns client id
func (c *client) ID() int64 {
	return c.id
}

// Utilities
