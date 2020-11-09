package etcdance

import (
	"go.etcd.io/etcd/clientv3"
)

// Client : etcdancer.Client
type Client interface {
}

type client struct {
	cli *clientv3.Client
	kvc clientv3.KV
}

// New : Create etcdance.Client from *etcd.clientv3.Client
func New(cli *clientv3.Client) Client {
	return &client{
		cli: cli,
		kvc: clientv3.NewKV(cli),
	}
}
