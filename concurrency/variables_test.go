package concurrency

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	wsl2host = "172.31.77.150"
)

var (
	endpoints = []string{
		// wsl2
		fmt.Sprintf("%s:%s", wsl2host, "2301"),
		fmt.Sprintf("%s:%s", wsl2host, "2302"),
		fmt.Sprintf("%s:%s", wsl2host, "2303"),
		// Xiaomi
		// "192.168.31.21:2301",
		// "192.168.31.21:2302",
		// "192.168.31.21:2303",
		// ZeroTier
		// "192.168.196.200:2301",
		// "192.168.196.200:2302",
		// "192.168.196.200:2303",
	}
)

func init() {
	rand.Seed(time.Now().Unix())
}
