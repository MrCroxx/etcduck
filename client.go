package etcdance

import (
	"github.com/MrCroxx/etcdance/snowflake"
	"go.etcd.io/etcd/clientv3"
)

// Client : etcdancer.Client
type Client interface {
}

type client struct {
	id  int64
	cli *clientv3.Client
	kvc clientv3.KV
}

// New : Create etcdance.Client from *etcd.clientv3.Client
func New(cli *clientv3.Client, datacenterID int64, workerID int64) (Client, error) {
	snowflake.InitOrReset(datacenterID, workerID)
	id, err := snowflake.NextID()
	if err != nil || id == -1 {
		return nil, err
	}
	return &client{
		id:  id,
		cli: cli,
		kvc: clientv3.NewKV(cli),
	}, nil
}

// Implement

// Utilities
