package godb

import (
	"testing"
)

// This function is for _testing only_!  It is not part of the godb API.
func insertTupleForTest(t *testing.T, hf DBFile, tup *Tuple, tid TransactionID) {
	t.Helper()
	err := hf.insertTuple(tup, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestFilterInt(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)

	insertTupleForTest(t, hf, &t1, tid)
	insertTupleForTest(t, hf, &t2, tid)

	var f FieldType = FieldType{"age", "", IntType}
	filt, err := NewFilter(&ConstExpr{IntField{25}, IntType}, OpGt, &FieldExpr{f}, hf)
	if err != nil {
		t.Errorf(err.Error())
	}
	iter, err := filt.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}

	cnt := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		t.Logf("filter passed tup %d: %v\n", cnt, tup)
		cnt++
	}
	if cnt != 1 {
		t.Errorf("unexpected number of results")
	}
}

func TestFilterString(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	insertTupleForTest(t, hf, &t1, tid)
	insertTupleForTest(t, hf, &t2, tid)
	var f FieldType = FieldType{"name", "", StringType}
	filt, err := NewFilter(&ConstExpr{StringField{"sam"}, StringType}, OpEq, &FieldExpr{f}, hf)
	if err != nil {
		t.Errorf(err.Error())
	}
	iter, err := filt.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}

	cnt := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		t.Logf("filter passed tup %d: %v\n", cnt, tup)
		cnt++
	}
	if cnt != 1 {
		t.Errorf("unexpected number of results")
	}
}
