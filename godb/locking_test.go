package godb

import (
	"sync"
	"testing"
	"time"
)

type LockGrabber struct {
	bp   *BufferPool
	tid  TransactionID
	file DBFile
	pgNo int
	perm RWPerm

	acq          bool
	err          error
	alock, elock sync.Mutex
}

func NewLockGrabber(bp *BufferPool, tid TransactionID, file DBFile, pgNo int, perm RWPerm) *LockGrabber {
	return &LockGrabber{bp, tid, file, pgNo, perm,
		false, nil, sync.Mutex{}, sync.Mutex{}}
}

func (lg *LockGrabber) run() {
	// Try to get the page from the buffer pool.
	_, err := lg.bp.GetPage(lg.file, lg.pgNo, lg.tid, lg.perm)
	if err == nil {
		lg.alock.Lock()
		lg.acq = true
		lg.alock.Unlock()
	} else {
		lg.elock.Lock()
		lg.err = err
		lg.elock.Unlock()

		lg.bp.AbortTransaction(lg.tid)
	}
}

func (lg *LockGrabber) acquired() bool {
	lg.alock.Lock()
	defer lg.alock.Unlock()
	return lg.acq
}

func (lg *LockGrabber) getError() error {
	lg.elock.Lock()
	defer lg.elock.Unlock()
	return lg.err
}

func startGrabber(bp *BufferPool, tid TransactionID, file DBFile, pgNo int, perm RWPerm) *LockGrabber {
	lg := NewLockGrabber(bp, tid, file, pgNo, perm)
	go lg.run()
	return lg
}

func grabLock(t *testing.T,
	bp *BufferPool, tid TransactionID, file DBFile, pgNo int, perm RWPerm,
	expected bool) {

	lg := startGrabber(bp, tid, file, pgNo, perm)

	time.Sleep(100 * time.Millisecond)

	var acquired bool = lg.acquired()
	if expected != acquired {
		t.Errorf("Expected %t, found %t", expected, acquired)
	}

	// TODO how to kill stalling lg?
}

func metaLockTester(t *testing.T, bp *BufferPool,
	tid1 TransactionID, file1 DBFile, pgNo1 int, perm1 RWPerm,
	tid2 TransactionID, file2 DBFile, pgNo2 int, perm2 RWPerm,
	expected bool) {
	bp.GetPage(file1, pgNo1, tid1, perm1)
	grabLock(t, bp, tid2, file2, pgNo2, perm2, expected)
}

func lockingTestSetUp(t *testing.T) (*BufferPool, *HeapFile, TransactionID, TransactionID) {
	bp, hf, tid1, tid2, _ := transactionTestSetUp(t)
	return bp, hf, tid1, tid2
}

func TestAcquireReadLocksOnSamePage(t *testing.T) {
	bp, hf, tid1, tid2 := lockingTestSetUp(t)
	metaLockTester(t, bp,
		tid1, hf, 0, ReadPerm,
		tid2, hf, 0, ReadPerm,
		true)
}

func TestAcquireReadWriteLocksOnSamePage(t *testing.T) {
	bp, hf, tid1, tid2 := lockingTestSetUp(t)
	metaLockTester(t, bp,
		tid1, hf, 0, ReadPerm,
		tid2, hf, 0, WritePerm,
		false)
}

func TestAcquireWriteReadLocksOnSamePage(t *testing.T) {
	bp, hf, tid1, tid2 := lockingTestSetUp(t)
	metaLockTester(t, bp,
		tid1, hf, 0, WritePerm,
		tid2, hf, 0, ReadPerm,
		false)
}

func TestAcquireReadWriteLocksOnTwoPages(t *testing.T) {
	bp, hf, tid1, tid2 := lockingTestSetUp(t)
	metaLockTester(t, bp,
		tid1, hf, 0, ReadPerm,
		tid2, hf, 1, WritePerm,
		true)
}

func TestAcquireWriteLocksOnTwoPages(t *testing.T) {
	bp, hf, tid1, tid2 := lockingTestSetUp(t)
	metaLockTester(t, bp,
		tid1, hf, 0, WritePerm,
		tid2, hf, 1, WritePerm,
		true)
}

func TestAcquireReadLocksOnTwoPages(t *testing.T) {
	bp, hf, tid1, tid2 := lockingTestSetUp(t)
	metaLockTester(t, bp,
		tid1, hf, 0, ReadPerm,
		tid2, hf, 1, ReadPerm,
		true)
}

func TestLockUpgrade(t *testing.T) {
	bp, hf, tid1, tid2 := lockingTestSetUp(t)
	metaLockTester(t, bp,
		tid1, hf, 0, ReadPerm,
		tid1, hf, 0, WritePerm,
		true)
	metaLockTester(t, bp,
		tid2, hf, 1, ReadPerm,
		tid2, hf, 1, WritePerm,
		true)
}

func TestAcquireWriteAndReadLocks(t *testing.T) {
	bp, hf, tid1, _ := lockingTestSetUp(t)
	metaLockTester(t, bp,
		tid1, hf, 0, WritePerm,
		tid1, hf, 0, ReadPerm,
		true)
}
