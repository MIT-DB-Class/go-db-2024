package godb

import (
	"testing"
)

func testLimitCount(t *testing.T, n int) {
	t.Helper()
	_, t1, t2, hf, bp, _ := makeTestVars(t)

	for i := 0; i < n; i++ {
		tid := NewTID()
		bp.BeginTransaction(tid)
		err := hf.insertTuple(&t1, tid)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		err = hf.insertTuple(&t2, tid)
		if err != nil {
			t.Errorf(err.Error())
			return
		}

		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 2
		if i%10 == 0 {
			bp.FlushAllPages()
		}

		//commit frequently to prevent buffer pool from filling
		bp.CommitTransaction(tid)
	}

	// check results
	tid := NewTID()
	bp.BeginTransaction(tid)
	lim := NewLimitOp(&ConstExpr{IntField{int64(n)}, IntType}, hf)
	if lim == nil {
		t.Fatalf("Op was nil")
		return
	}
	iter, err := lim.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
		return
	}

	cnt := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		cnt++
	}
	if cnt != n {
		t.Errorf("unexpected number of results")
	}

	bp.CommitTransaction(tid)
}

func TestLimit5(t *testing.T) {
	testLimitCount(t, 5)
}

func TestLimit50(t *testing.T) {
	testLimitCount(t, 50)
}

func TestLimit100(t *testing.T) {
	testLimitCount(t, 100)
}
