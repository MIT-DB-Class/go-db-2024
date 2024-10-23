package godb

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

const POLL_INTERVAL = 100 * time.Millisecond
const WAIT_INTERVAL = 200 * time.Millisecond

/**
* Not-so-unit test to construct a deadlock situation.
* t1 acquires p0.read; t2 acquires p1.read; t1 attempts p1.write; t2
* attempts p0.write. Rinse and repeat.
 */
func TestDeadlockReadWrite(t *testing.T) {
	bp, hf, tid1, tid2 := lockingTestSetUp(t)

	lg1Read := startGrabber(bp, tid1, hf, 0, ReadPerm)
	lg2Read := startGrabber(bp, tid2, hf, 1, ReadPerm)

	time.Sleep(POLL_INTERVAL)

	lg1Write := startGrabber(bp, tid1, hf, 1, WritePerm)
	lg2Write := startGrabber(bp, tid2, hf, 0, WritePerm)

	for {
		time.Sleep(POLL_INTERVAL)

		if lg1Write.acquired() && lg2Write.acquired() {
			t.Errorf("Should not both get write lock")
		}
		if lg1Write.acquired() != lg2Write.acquired() {
			break
		}

		if lg1Write.getError() != nil {
			bp.AbortTransaction(tid1) // at most abort twice; should be able to abort twice
			time.Sleep(time.Duration((float64(WAIT_INTERVAL) * rand.Float64())))

			tid1 = NewTID()
			lg1Read = startGrabber(bp, tid1, hf, 0, ReadPerm)
			time.Sleep(POLL_INTERVAL)
			lg1Write = startGrabber(bp, tid1, hf, 1, WritePerm)
		}

		if lg2Write.getError() != nil {
			bp.AbortTransaction(tid2) // at most abort twice; should be able to abort twice
			time.Sleep(time.Duration((float64(WAIT_INTERVAL) * rand.Float64())))

			tid2 = NewTID()
			lg2Read = startGrabber(bp, tid2, hf, 1, ReadPerm)
			time.Sleep(POLL_INTERVAL)
			lg2Write = startGrabber(bp, tid2, hf, 0, WritePerm)
		}
	}

	if lg1Read == nil || lg2Read == nil {
		fmt.Println("should not be nil")
	}
}

/**
 * Not-so-unit test to construct a deadlock situation.
 * t1 acquires p0.write; t2 acquires p1.write; t1 attempts p1.write; t2
 * attempts p0.write.
 */
func TestDeadlockWriteWrite(t *testing.T) {
	bp, hf, tid1, tid2 := lockingTestSetUp(t)

	lg1WriteA := startGrabber(bp, tid1, hf, 0, WritePerm)
	lg2WriteA := startGrabber(bp, tid2, hf, 1, WritePerm)

	time.Sleep(POLL_INTERVAL)

	lg1WriteB := startGrabber(bp, tid1, hf, 1, WritePerm)
	lg2WriteB := startGrabber(bp, tid2, hf, 0, WritePerm)

	for {
		time.Sleep(POLL_INTERVAL)

		if lg1WriteB.acquired() && lg2WriteB.acquired() {
			t.Errorf("Should not both get write lock")
		}
		if lg1WriteB.acquired() != lg2WriteB.acquired() {
			break
		}

		if lg1WriteB.getError() != nil {
			bp.AbortTransaction(tid1) // at most abort twice; should be able to abort twice
			time.Sleep(time.Duration((float64(WAIT_INTERVAL) * rand.Float64())))

			tid1 = NewTID()
			lg1WriteA = startGrabber(bp, tid1, hf, 0, WritePerm)
			time.Sleep(POLL_INTERVAL)
			lg1WriteB = startGrabber(bp, tid1, hf, 1, WritePerm)
		}

		if lg2WriteB.getError() != nil {
			bp.AbortTransaction(tid2) // at most abort twice; should be able to abort twice
			time.Sleep(time.Duration((float64(WAIT_INTERVAL) * rand.Float64())))

			tid2 = NewTID()
			lg2WriteA = startGrabber(bp, tid2, hf, 1, WritePerm)
			time.Sleep(POLL_INTERVAL)
			lg2WriteB = startGrabber(bp, tid2, hf, 0, WritePerm)
		}
	}

	if lg1WriteA == nil || lg2WriteA == nil {
		fmt.Println("should not be nil")
	}
}

/**
 * Not-so-unit test to construct a deadlock situation.
 * t1 acquires p0.read; t2 acquires p0.read; t1 attempts to upgrade to
 * p0.write; t2 attempts to upgrade to p0.write
 */
func TestDeadlockUpgradeWrite(t *testing.T) {
	bp, hf, tid1, tid2 := lockingTestSetUp(t)

	lg1Read := startGrabber(bp, tid1, hf, 0, ReadPerm)
	lg2Read := startGrabber(bp, tid2, hf, 0, ReadPerm)

	time.Sleep(POLL_INTERVAL)

	lg1Write := startGrabber(bp, tid1, hf, 0, WritePerm)
	lg2Write := startGrabber(bp, tid2, hf, 0, WritePerm)

	for {
		time.Sleep(POLL_INTERVAL)

		if lg1Write.acquired() && lg2Write.acquired() {
			t.Errorf("Should not both get write lock")
		}
		if lg1Write.acquired() != lg2Write.acquired() {
			break
		}

		if lg1Write.getError() != nil {
			bp.AbortTransaction(tid1) // at most abort twice; should be able to abort twice
			time.Sleep(time.Duration((float64(WAIT_INTERVAL) * rand.Float64())))

			tid1 = NewTID()
			lg1Read = startGrabber(bp, tid1, hf, 0, ReadPerm)
			time.Sleep(POLL_INTERVAL)
			lg1Write = startGrabber(bp, tid1, hf, 0, WritePerm)
		}

		if lg2Write.getError() != nil {
			bp.AbortTransaction(tid2) // at most abort twice; should be able to abort twice
			time.Sleep(time.Duration((float64(WAIT_INTERVAL) * rand.Float64())))

			tid2 = NewTID()
			lg2Read = startGrabber(bp, tid2, hf, 0, ReadPerm)
			time.Sleep(POLL_INTERVAL)
			lg2Write = startGrabber(bp, tid2, hf, 0, WritePerm)
		}
	}

	if lg1Read == nil || lg2Read == nil {
		fmt.Println("should not be nil")
	}
}
