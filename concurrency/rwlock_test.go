package concurrency

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v3 "go.etcd.io/etcd/clientv3"
)

func TestRWLockSum(t *testing.T) {
	assert := assert.New(t)

	client, err := v3.New(v3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Minute * 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	target := 100

	for {

		sum := 0
		fakesum := 0
		target *= 2
		var wg sync.WaitGroup

		for i := 0; i < target; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				fakesum++

				t.Logf("create rwlock object")
				res := "sum"
				rw, err := NewRWLock(client, res)
				if err != nil {
					t.Fatal(err)
				}

				t.Logf("acquire lock on <%s>", res)
				rwinfo, err := rw.Lock(context.TODO(), 0)
				if err != nil {
					t.Fatal(err)
				}
				key := fmt.Sprintf("%s/%s", rwinfo.RKey, rwinfo.WKey)
				t.Logf("lock <%s> on <%s> acquired", key, rwinfo.Resource)

				sum++

				t.Logf("release lock on <%s>", rwinfo.Resource)
				err = rw.Unlock(context.TODO())
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("lock <%s> on <%s> released", key, rwinfo.Resource)
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

func TestRWLockReentrant(t *testing.T) {

	assert := assert.New(t)

	client, err := v3.New(v3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Minute * 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	rw, err := NewRWLock(client, "reentrant")

	_, err = rw.Lock(context.TODO(), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("acquired rwlock.lock once, go on acquiring")
	_, err = rw.Lock(context.TODO(), 0)
	assert.Errorf(err, "rwlock.Lock is supposed to return an error when called reentrantly")
}

func TestRWLockDirectlyUnlock(t *testing.T) {

	assert := assert.New(t)

	client, err := v3.New(v3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Minute * 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	rw, err := NewRWLock(client, "reentrant")

	err = rw.Unlock(context.TODO())
	assert.Errorf(err, "rwlock.Lock is supposed to return an error when called reentrantly")
}
