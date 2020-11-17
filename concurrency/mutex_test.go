package concurrency

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v3 "go.etcd.io/etcd/clientv3"
)

func TestMutexSum(t *testing.T) {
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

				t.Logf("create mutex object")
				m, err := NewMutex(client, "sum")
				if err != nil {
					t.Fatal(err)
				}

				t.Logf("acquire lock on <%s>", m.Resource())
				err = m.Lock(context.TODO(), 0)
				if err != nil {
					t.Fatal(err)
				}
				key := m.Key()
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

func TestMutexReentrant(t *testing.T) {

	assert := assert.New(t)

	client, err := v3.New(v3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Minute * 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	m, err := NewMutex(client, "reentrant")

	err = m.Lock(context.TODO(), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("acquired mutex.lock once, go on acquiring")
	err = m.Lock(context.TODO(), 0)
	assert.Errorf(err, "mutex.Lock is supposed to return an error when called reentrantly")
}

func TestMutexDirectlyUnlock(t *testing.T) {

	assert := assert.New(t)

	client, err := v3.New(v3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Minute * 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	m, err := NewMutex(client, "reentrant")

	err = m.Unlock(context.TODO())
	assert.Errorf(err, "mutex.Lock is supposed to return an error when called reentrantly")
}

func TestMutexTimeout(t *testing.T) {
	assert := assert.New(t)

	client, err := v3.New(v3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Minute * 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	m1, err := NewRWLock(client, "to")
	if err != nil || m1 == nil {
		t.Fatal(err)
	}
	m2, err := NewRWLock(client, "to")
	if err != nil || m2 == nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	ch := make(chan int)
	wg.Add(1)

	go func() {
		if _, err := m1.Lock(context.TODO(), 0); err != nil {
			t.Fatal(err)
		}
		t.Logf("m1 acquired mutex")
		ch <- 0
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		<-ch
		_, err := m2.Lock(context.TODO(), time.Second*3)
		if err == nil {
			t.Logf("m2 acquired mutex")
		}
		t.Log(err)
		assert.Errorf(err, "expected timeout error")
		wg.Done()
	}()
	wg.Wait()
}
