package godb

import (
	"os"
	"testing"
)

const InsertTestFile string = "InsertTestFile.dat"

func TestInsert(t *testing.T) {
	td, t1, _, hf, bp, tid := makeTestVars(t)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t1, tid)
	bp.CommitTransaction(tid)
	os.Remove(InsertTestFile)
	hf2, _ := NewHeapFile(InsertTestFile, &td, bp)
	if hf2 == nil {
		t.Fatalf("hf was nil")
	}
	tid = NewTID()
	bp.BeginTransaction(tid)
	ins := NewInsertOp(hf2, hf)
	iter, _ := ins.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	if tup == nil {
		t.Errorf("insert did not return tuple")
		return
	}
	intField, ok := tup.Fields[0].(IntField)
	if !ok || len(tup.Fields) != 1 || intField.Value != 2 {
		t.Errorf("invalid output tuple")
		return
	}
	bp.CommitTransaction(tid)
	tid = NewTID()
	bp.BeginTransaction(tid)

	cnt := 0
	iter, _ = hf2.Iterator(tid)
	for {
		tup, err := iter()

		if err != nil {
			t.Errorf(err.Error())
		}
		if tup == nil {
			break
		}
		cnt = cnt + 1
	}
	if cnt != 2 {
		t.Errorf("insert failed, expected 2 tuples, got %d", cnt)
	}
}
