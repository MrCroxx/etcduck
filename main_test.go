package etcduck

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/MrCroxx/etcduck/concurrency"
	"github.com/stretchr/testify/assert"
	v3 "go.etcd.io/etcd/clientv3"
)

var (
	endpoints = []string{
		// Xiaomi
		"192.168.31.21:2301",
		"192.168.31.21:2302",
		"192.168.31.21:2303",
		// ZeroTier
		// "192.168.196.200:2301",
		// "192.168.196.200:2302",
		// "192.168.196.200:2303",
	}
)

func TestMutex(t *testing.T) {
	assert := assert.New(t)

	client, err := v3.New(v3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Minute * 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	rand.Seed(time.Now().Unix())

	for {

		sum := 0
		fakesum := 0
		target := 500
		var wg sync.WaitGroup

		for i := 0; i < target; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				fakesum++

				t.Logf("create mutex object")
				m, err := concurrency.NewMutex(client, "sum")
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("mutex <%s> on <%s> created", m.Key(), m.Resource())

				t.Logf("acquire lock on <%s>", m.Resource())
				err = m.Lock(context.TODO(), 0)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("lock <%s> on <%s> acquired", m.Key(), m.Resource())

				sum++

				t.Logf("release lock on <%s>", m.Resource())
				err = m.Unlock(context.TODO())
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("lock <%s> on <%s> released", m.Key(), m.Resource())
			}(i)
		}
		wg.Wait()
		assert.Equal(sum, target, "<sum> and <target> mismatch")
		if sum != fakesum {
			t.Logf("sum:<%d> fakesum:<%d>", sum, fakesum)
			break
		}
		t.Logf("<sum> equals <fakesum>, need another loop to assert")
	}

}
