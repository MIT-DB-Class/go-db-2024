package godb

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

// This test does not have credit and serves merely as a sanity check since
// NewTID() should have been implemented for you.
func TestTransactionTid(t *testing.T) {
	tid := NewTID()
	tid2 := NewTID()
	var tid3 = tid
	if tid == tid2 {
		t.Errorf("different transactions have same id")
	}
	if tid != tid3 {
		t.Errorf("same transactions have different id")
	}
}

const numConcurrentThreads int = 20

var c chan int = make(chan int, numConcurrentThreads*2)

func readXaction(hf DBFile, bp *BufferPool, wg *sync.WaitGroup) {
	for {
	start:
		tid := NewTID()
		bp.BeginTransaction(tid)
		pgCnt1 := hf.NumPages()
		it, _ := hf.Iterator(tid)
		cnt1 := 0

		for {
			t, err := it()
			if err != nil {
				// Assume this is because of a deadlock, restart txn
				time.Sleep(time.Duration(rand.Intn(8)) * 100 * time.Microsecond)
				goto start
			}
			if t == nil {
				break
			}
			cnt1++
		}

		it, _ = hf.Iterator(tid)
		cnt2 := 0
		for {
			t, err := it()
			if err != nil {
				// Assume this is because of a deadlock, restart txn
				time.Sleep(time.Duration(rand.Intn(8)) * 100 * time.Microsecond)
				goto start
			}
			if t == nil {
				break
			}
			cnt2++
		}
		if cnt1 == cnt2 || pgCnt1 != hf.NumPages() {
			//fmt.Printf("read same number of tuples both iterators (%d)\n", cnt1)
			c <- 1
		} else {
			fmt.Printf("ERROR: read different number of tuples both iterators (%d, %d)\n", cnt1, cnt2)
			c <- 0
		}
		bp.CommitTransaction(tid)
		wg.Done()
		return
	}
}

func writeXaction(hf DBFile, bp *BufferPool, writeTuple Tuple, wg *sync.WaitGroup) {
	for {
	start:
		tid := NewTID()
		bp.BeginTransaction(tid)
		for i := 0; i < 10; i++ {
			err := hf.insertTuple(&writeTuple, tid)
			if err != nil {
				// Assume this is because of a deadlock, restart txn
				time.Sleep(time.Duration(rand.Intn(8)) * 100 * time.Microsecond)
				goto start
			}
		}
		bp.CommitTransaction(tid)
		break
	}
	c <- 1
	wg.Done()
}

func TestTransactions(t *testing.T) {
	_, t1, t2, _, _, _ := makeTestVars(t)
	bp, catalog, err := MakeTestDatabase(20, "catalog.txt")
	if err != nil {
		t.Fatalf(err.Error())
	}

	tid := NewTID()
	bp.BeginTransaction(tid)
	hf, err := catalog.GetTable("t")
	if err != nil {
		t.Fatalf(err.Error())
	}
	var wg sync.WaitGroup

	for i := 0; i < 1000; i++ {
		err := hf.insertTuple(&t1, tid)
		if err != nil {
			fmt.Print(err.Error())
			t.Errorf("transaction test failed")
		}
		err = hf.insertTuple(&t2, tid)
		if err != nil {
			fmt.Print(err.Error())
			t.Errorf("transaction test failed")
		}
	}
	bp.CommitTransaction(tid)

	wg.Add(numConcurrentThreads * 2)

	for i := 0; i < numConcurrentThreads; i++ {
		go readXaction(hf, bp, &wg)
		//time.Sleep(2 * time.Millisecond)
		go writeXaction(hf, bp, t1, &wg)
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()

	for i := 0; i < numConcurrentThreads*2; i++ {
		val := <-c
		if val == 0 {
			t.Errorf("transaction test failed")
		}
	}

	wg.Add(1)
	go readXaction(hf, bp, &wg)
	wg.Wait()
}

func transactionTestSetUpVarLen(t *testing.T, tupCnt int, pgCnt int) (*BufferPool, *HeapFile, TransactionID, TransactionID, Tuple, Tuple) {
	_, t1, t2, hf, bp, _ := makeTestVars(t)

	csvFile, err := os.Open(fmt.Sprintf("txn_test_%d_%d.csv", tupCnt, pgCnt))
	if err != nil {
		t.Fatalf("error opening test file")
	}
	hf.LoadFromCSV(csvFile, false, ",", false)
	if hf.NumPages() != pgCnt {
		t.Fatalf("error making test vars; unexpected number of pages")
	}

	tid1 := NewTID()
	bp.BeginTransaction(tid1)
	tid2 := NewTID()
	bp.BeginTransaction(tid2)
	return bp, hf, tid1, tid2, t1, t2
}

func transactionTestSetUp(t *testing.T) (*BufferPool, *HeapFile, TransactionID, TransactionID, Tuple) {
	bp, hf, tid1, tid2, t1, _ := transactionTestSetUpVarLen(t, 300, 3)
	return bp, hf, tid1, tid2, t1
}

func TestTransactionTwice(t *testing.T) {
	bp, hf, tid1, tid2, _ := transactionTestSetUp(t)
	bp.GetPage(hf, 0, tid1, ReadPerm)
	bp.GetPage(hf, 1, tid1, WritePerm)
	bp.CommitTransaction(tid1)

	bp.GetPage(hf, 0, tid2, WritePerm)
	bp.GetPage(hf, 1, tid2, WritePerm)
}

func testTransactionComplete(t *testing.T, commit bool) {
	bp, hf, tid1, tid2, t1 := transactionTestSetUp(t)

	pg, _ := bp.GetPage(hf, 2, tid1, WritePerm)
	heapp := pg.(*heapPage)
	heapp.insertTuple(&t1)
	heapp.setDirty(tid1, true)

	if commit {
		bp.CommitTransaction(tid1)
	} else {
		bp.AbortTransaction(tid1)
	}

	bp.FlushAllPages()

	pg, _ = bp.GetPage(hf, 2, tid2, WritePerm)
	heapp = pg.(*heapPage)
	iter := heapp.tupleIter()

	found := false
	for tup, err := iter(); tup != nil || err != nil; tup, err = iter() {
		if err != nil {
			t.Fatalf("Iterator error")
		}
		if t1.equals(tup) {
			found = true
			break
		}
	}

	if found != commit {
		t.Errorf("Expected %t, found %t", commit, found)
	}
}

func TestTransactionCommit(t *testing.T) {
	testTransactionComplete(t, true)
}

func TestTransactionAbort(t *testing.T) {
	testTransactionComplete(t, false)
}

// placeholder op for a singleton tuple
type Singleton struct {
	tup Tuple
	ran bool
}

func (i *Singleton) Descriptor() *TupleDesc {
	return &i.tup.Desc
}

func (i *Singleton) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	return func() (*Tuple, error) {
		if i.ran {
			return nil, nil
		}
		i.ran = true
		return &i.tup, nil
	}, nil
}

// Run threads transactions, each each of which reads a single tuple from a
// page, deletes the tuple, and re-inserts it with an incremented value. There
// will be deadlocks, so your deadlock handling will have to be correct to allow
// all transactions to be committed and the value to be incremented threads
// times.
func validateTransactions(t *testing.T, threads int) {
	bp, hf, _, _, _, t2 := transactionTestSetUpVarLen(t, 1, 1)

	var startWg, readyWg sync.WaitGroup
	startChan := make(chan struct{})

	// sleep for an increasingly long time after deadlocks. this backoff helps avoid starvation
	nDeadlocks := 0
	var nDeadlocksMutex sync.Mutex
	sleepAfterDeadlock := func(thrId int, err error) {
		nDeadlocksMutex.Lock()
		nDeadlocks++
		t.Logf("thread %d operation failed: %v deadlock #%v", thrId, err, nDeadlocks)
		sleepTime := time.Duration(rand.Intn(int(nDeadlocks) + 1))
		nDeadlocksMutex.Unlock()
		time.Sleep(sleepTime * time.Millisecond)
	}

	incrementer := func(thrId int) {
		// Signal that this goroutine is ready
		readyWg.Done()

		// Wait for the signal to start
		<-startChan

		for tid := TransactionID(0); ; bp.AbortTransaction(tid) {
			tid = NewTID()
			bp.BeginTransaction(tid)
			iter1, err := hf.Iterator(tid)
			if err != nil {
				continue
			}

			readTup, err := iter1()
			if err != nil {
				sleepAfterDeadlock(thrId, err)
				continue
			}

			var writeTup = Tuple{
				Desc: readTup.Desc,
				Fields: []DBValue{
					readTup.Fields[0],
					IntField{readTup.Fields[1].(IntField).Value + 1},
				}}

			dop := NewDeleteOp(hf, hf)
			iterDel, err := dop.Iterator(tid)
			if err != nil {
				continue
			}
			delCnt, err := iterDel()
			if err != nil {
				sleepAfterDeadlock(thrId, err)
				continue
			}
			if delCnt.Fields[0].(IntField).Value != 1 {
				t.Errorf("Delete Op should return 1")
			}
			iop := NewInsertOp(hf, &Singleton{writeTup, false})
			iterIns, err := iop.Iterator(tid)
			if err != nil {
				continue
			}
			insCnt, err := iterIns()
			if err != nil {
				sleepAfterDeadlock(thrId, err)
				continue
			}

			if insCnt.Fields[0].(IntField).Value != 1 {
				t.Errorf("Insert Op should return 1")
			}

			bp.CommitTransaction(tid)
			break //exit on success, so we don't do terminal abort
		}
		startWg.Done()
	}

	// Prepare goroutines
	readyWg.Add(threads)
	startWg.Add(threads)
	for i := 0; i < threads; i++ {
		go incrementer(i)
	}

	// Wait for all goroutines to be ready
	readyWg.Wait()

	// Start all goroutines at once
	close(startChan)

	// Wait for all goroutines to finish
	startWg.Wait()

	tid := NewTID()
	bp.BeginTransaction(tid)
	iter, _ := hf.Iterator(tid)
	tup, _ := iter()

	diff := tup.Fields[1].(IntField).Value - t2.Fields[1].(IntField).Value
	if diff != int64(threads) {
		t.Errorf("Expected #increments = %d, found %d", threads, diff)
	}
}

func TestTransactionSingleThread(t *testing.T) {
	validateTransactions(t, 1)
}

func TestTransactionTwoThreads(t *testing.T) {
	validateTransactions(t, 2)
}

func TestTransactionFiveThreads(t *testing.T) {
	validateTransactions(t, 5)
}

func TestTransactionAllDirtyFails(t *testing.T) {
	if os.Getenv("LAB") == "5" {
		t.Skip("Test is valid up through Lab 4. Skipping.")
	}
	td, t1, _, hf, bp, tid := makeTestVars(t)

	for hf.NumPages() < 3 {
		hf.insertTuple(&t1, tid)
		if hf.NumPages() == 0 {
			t.Fatalf("Heap file should have at least one page after insertion.")
		}
	}
	bp.CommitTransaction(tid) // make three clean pages

	os.Remove(TestingFile2)
	hf2, _ := NewHeapFile(TestingFile2, &td, bp)
	tid2 := NewTID()
	bp.BeginTransaction(tid2)

	for hf2.NumPages() < 3 { // make three dirty pages
		hf2.insertTuple(&t1, tid2)
		if hf2.NumPages() == 0 {
			t.Fatalf("Heap file should have at least one page after insertion.")
		}
	}

	_, err := bp.GetPage(hf, 0, tid2, ReadPerm) // since bp capacity = 3, should return error due to all dirty pages
	if err == nil {
		t.Errorf("Expected error due to all dirty pages")
	}
}

func TestTransactionAbortEviction(t *testing.T) {
	tupExists := func(t0 Tuple, tid TransactionID, hf *HeapFile) (bool, error) {
		iter, err := hf.Iterator(tid)
		if err != nil {
			return false, err
		}
		for tup, err := iter(); tup != nil; tup, err = iter() {
			if err != nil {
				return false, err
			}
			if t0.equals(tup) {
				return true, nil
			}
		}
		return false, nil
	}

	_, t1, _, hf, bp, tid := makeTestVars(t)
	hf.insertTuple(&t1, tid)
	if exists, err := tupExists(t1, tid, hf); !(exists == true && err == nil) {
		t.Errorf("Tuple should exist")
	}
	bp.AbortTransaction(tid)

	tid2 := NewTID()
	bp.BeginTransaction(tid2)

	// tuple should not exist after abort
	if exists, err := tupExists(t1, tid2, hf); !(exists == false && err == nil) {
		t.Errorf("Tuple should not exist")
	}
}
