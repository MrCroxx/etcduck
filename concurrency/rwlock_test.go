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
				m, err := NewRWLock(client, "sum")
				if err != nil {
					t.Fatal(err)
				}

				t.Logf("acquire lock on <%s>", m.Resource())
				err = m.Lock(context.TODO(), 0)
				if err != nil {
					t.Fatal(err)
				}
				key := fmt.Sprintf("%s/%s", m.RKey(), m.WKey())
				t.Logf("lock <%s> on <%s> acquired", key, m.Resource())

				sum++

				t.Logf("release lock on <%s>", m.Resource())
				err = m.Unlock(context.TODO())
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("lock <%s> on <%s> released", key, m.Resource())
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

	m, err := NewRWLock(client, "reentrant")

	err = m.Lock(context.TODO(), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("acquired rwlock.lock once, go on acquiring")
	err = m.Lock(context.TODO(), 0)
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

	m, err := NewRWLock(client, "reentrant")

	err = m.Unlock(context.TODO())
	assert.Errorf(err, "rwlock.Lock is supposed to return an error when called reentrantly")
}
