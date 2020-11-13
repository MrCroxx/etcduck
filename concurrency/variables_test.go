package concurrency

import (
	"math/rand"
	"time"
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

func init() {
	rand.Seed(time.Now().Unix())
}
