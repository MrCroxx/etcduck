package snowflake

import (
	"fmt"
	"sync"
	"time"
)

const (
	cTwepoch int64 = 1420041600000

	cWorkerIDBits     int64 = 5
	cDatacenterIDBits int64 = 5
	cSequenceBits     int64 = 12

	cWorkerIDShift      int64 = cSequenceBits
	cDatacenterIDShift  int64 = cSequenceBits + cWorkerIDBits
	cTimestampLeftShift int64 = cSequenceBits + cWorkerIDBits + cDatacenterIDBits

	cMaxWorkerID     int64 = -1 ^ (-1 << cWorkerIDBits)
	cMaxDatacenterID int64 = -1 ^ (-1 << cDatacenterIDBits)

	cSequenceMask int64 = -1 ^ (-1 << cSequenceBits)
)

var (
	vWorkerID      int64
	vDatacenterID  int64
	vSequence      int64
	vLastTimestamp int64

	initilized bool

	mutex    sync.Mutex
	onceInit sync.Once
)

func init() {
	initilized = false
}

// InitOrReset : init or reset Snowflake module
func InitOrReset(datacenterID int64, workerID int64) {
	mutex.Lock()
	defer mutex.Unlock()

	vDatacenterID = datacenterID
	vWorkerID = workerID
	vLastTimestamp = -1
	vSequence = 0

	initilized = true
}

// NextID : generate next snowflake ID, returns -1 if error raised.
func NextID() (int64, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if !initilized {
		return -1, fmt.Errorf("snowflake module has not been initialized yet")
	}

	timestamp := time.Now().Unix()
	if timestamp < vLastTimestamp {
		return -1, fmt.Errorf("clock moved backwards. Refusing to generate id for %d milliseconds", vLastTimestamp-timestamp)
	}

	// If it is generated at the same time, the sequence in milliseconds is performed.
	if vLastTimestamp == timestamp {
		vSequence = (vSequence + 1) & cSequenceMask
		// Sequence overflow in milliseconds.
		if vSequence == 0 {
			// Block to the next millisecond until a new timestamp is obtained.
			for {
				timestamp = time.Now().Unix()
				if timestamp > vLastTimestamp {
					break
				}
			}
		}
	} else {
		// Timestamp changes, sequence reset in tnilliseconds.
		vSequence = 0
	}

	// Time cut of last ID generation.
	vLastTimestamp = timestamp

	// Shift and assemble 64-bit ID s together by operation or operation
	return ((timestamp - cTwepoch) << cTimestampLeftShift) |
			(vDatacenterID << cDatacenterIDShift) |
			(vWorkerID << cWorkerIDShift) |
			vSequence,
		nil
}
