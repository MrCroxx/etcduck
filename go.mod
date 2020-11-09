module github.com/MrCroxx/etcdance

go 1.15

require (
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/google/uuid v1.1.2 // indirect
	github.com/sirupsen/logrus v1.4.2
	go.etcd.io/etcd v3.3.25+incompatible
	google.golang.org/grpc v1.33.2 // indirect
)

replace google.golang.org/grpc v1.33.2 => google.golang.org/grpc v1.26.0

replace go.etcd.io/etcd => go.etcd.io/etcd v0.0.0-20200520232829-54ba9589114f
