package godb

import (
	"testing"
)

// This function is for _testing only_!  It is not part of the godb API.
func BeginTransactionForTest(t *testing.T, bp *BufferPool) TransactionID {
	t.Helper()
	tid := NewTID()
	err := bp.BeginTransaction(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	return tid
}

func TestDelete(t *testing.T) {
	_, t1, t2, hf, bp, tid := makeTestVars(t)

	insertTupleForTest(t, hf, &t1, tid)
	insertTupleForTest(t, hf, &t2, tid)

	bp.CommitTransaction(tid)
	var f FieldType = FieldType{"age", "", IntType}
	filt, err := NewFilter(&ConstExpr{IntField{25}, IntType}, OpGt, &FieldExpr{f}, hf)
	if err != nil {
		t.Errorf(err.Error())
	}
	dop := NewDeleteOp(hf, filt)
	if dop == nil {
		t.Fatalf("delete op was nil")
	}

	tid = BeginTransactionForTest(t, bp)
	iter, _ := dop.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("insert did not return tuple")
	}
	intField, ok := tup.Fields[0].(IntField)
	if !ok || len(tup.Fields) != 1 || intField.Value != 1 {
		t.Fatalf("invalid output tuple")
	}
	bp.CommitTransaction(tid)

	tid = BeginTransactionForTest(t, bp)

	iter, _ = hf.Iterator(tid)

	cnt := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		cnt++
	}
	if cnt != 1 {
		t.Errorf("unexpected number of results after deletion")
	}
}
