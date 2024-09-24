package godb

import (
	"fmt"
	"os"
	"testing"
	"time"
)

const JoinTestFile string = "JoinTestFile.dat"

func TestJoin(t *testing.T) {
	td, t1, t2, hf, bp, tid := makeTestVars(t)
	insertTupleForTest(t, hf, &t1, tid)
	insertTupleForTest(t, hf, &t2, tid)
	insertTupleForTest(t, hf, &t2, tid)

	os.Remove(JoinTestFile)
	hf2, _ := NewHeapFile(JoinTestFile, &td, bp)
	insertTupleForTest(t, hf2, &t1, tid)
	insertTupleForTest(t, hf2, &t2, tid)
	insertTupleForTest(t, hf2, &t2, tid)

	outT1 := joinTuples(&t1, &t1)
	outT2 := joinTuples(&t2, &t2)

	leftField := FieldExpr{td.Fields[1]}
	join, err := NewJoin(hf, &leftField, hf2, &leftField, 100)
	if err != nil {
		t.Errorf("unexpected error initializing join")
		return
	}
	iter, err := join.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	cnt := 0
	cntOut1 := 0
	cntOut2 := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		if t.equals(outT1) {
			cntOut1++
		} else if t.equals(outT2) {
			cntOut2++
		}
		//fmt.Printf("got tuple %v: %v\n", cnt, t)
		cnt++
	}
	if cnt != 5 {
		t.Errorf("unexpected number of join results (%d, expected 5)", cnt)
	}
	if cntOut1 != 1 {
		t.Errorf("unexpected number of t1 results (%d, expected 1)", cntOut1)
	}
	if cntOut2 != 4 {
		t.Errorf("unexpected number of t2 results (%d, expected 4)", cntOut2)
	}

}

const BigJoinFile1 string = "jointest1.dat"
const BigJoinFile2 string = "jointest2.dat"

//This test joins two large heap files (each containing ntups tuples). A simple
//nested loops join will take a LONG time to complete this join, so we've added
//a timeout that will cause the join to fail after 10 seconds.
//
//Note that this test is optional;  passing it will give extra credit, as
//describe in the lab 2 assignment.

func TestJoinBigOptional(t *testing.T) {
	timeout := time.After(20 * time.Second)

	done := make(chan bool)

	go func() {
		fail := func(err error) {
			done <- true
			t.Errorf(err.Error())
		}
		ntups := 314159

		if err := os.Remove(BigJoinFile1); err != nil && !os.IsNotExist(err) {
			fail(fmt.Errorf("removing file1: %w", err))
			return
		}
		if err := os.Remove(BigJoinFile2); err != nil && !os.IsNotExist(err) {
			fail(fmt.Errorf("removing file2: %w", err))
		}

		bp, c, err := MakeTestDatabase(100, "big_join_catalog.txt")
		if err != nil {
			fail(fmt.Errorf("making database: %w", err))
			return
		}

		hf1, err := c.GetTable("jointest1")
		if err != nil {
			fail(fmt.Errorf("getting jointest1: %w", err))
			return
		}
		hf2, err := c.GetTable("jointest2")
		if err != nil {
			fail(fmt.Errorf("getting jointest2: %w", err))
			return
		}

		tid := NewTID()
		bp.BeginTransaction(tid)
		for i := 0; i < ntups; i++ {

			if i > 0 && i%5000 == 0 {
				bp.FlushAllPages()
				// commit transaction
				bp.CommitTransaction(tid)

				tid = NewTID()
				bp.BeginTransaction(tid)
			}

			tup := Tuple{*hf1.Descriptor(), []DBValue{IntField{int64(i)}}, nil}
			err := hf1.insertTuple(&tup, tid)
			if err != nil {
				fail(fmt.Errorf("inserting tuple1: %w", err))
				return
			}

			err = hf2.insertTuple(&tup, tid)
			if err != nil {
				fail(fmt.Errorf("inserting tuple2: %w", err))
				return
			}
		}
		bp.CommitTransaction(tid)

		tid = NewTID()
		bp.BeginTransaction(tid)
		leftField := FieldExpr{hf1.Descriptor().Fields[0]}
		join, err := NewJoin(hf1, &leftField, hf2, &leftField, 100000)
		if err != nil {
			t.Errorf("unexpected error initializing join")
			done <- true
			return
		}
		iter, err := join.Iterator(tid)
		if err != nil {
			fail(err)
			return
		}

		if iter == nil {
			t.Errorf("iter was nil")
			done <- true
			return
		}
		cnt := 0
		for {
			tup, err := iter()
			if err != nil {
				fail(err)
				return
			}
			if tup == nil {
				break
			}
			cnt++
		}
		if cnt != ntups {
			t.Errorf("unexpected number of join results (%d, expected %d)", cnt, ntups)
		}
		done <- true
	}()

	select {
	case <-timeout:
		t.Fatal("Test didn't finish in time")
	case <-done:
	}
}

func makeJoinOrderingVars(t *testing.T) (*HeapFile, *HeapFile, Tuple, Tuple, *BufferPool) {
	var td1 = TupleDesc{Fields: []FieldType{
		{Fname: "a", Ftype: StringType},
		{Fname: "b", Ftype: IntType},
	}}
	var td2 = TupleDesc{Fields: []FieldType{
		{Fname: "c", Ftype: StringType},
		{Fname: "d", Ftype: IntType},
	}}

	var t1 = Tuple{
		Desc: td1,
		Fields: []DBValue{
			StringField{"sam"},
			IntField{25},
		}}

	var t2 = Tuple{
		Desc: td2,
		Fields: []DBValue{
			StringField{"george jones"},
			IntField{25},
		}}

	bp, err := NewBufferPool(3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	os.Remove(TestingFile)
	hf1, err := NewHeapFile(TestingFile, &td1, bp)
	if err != nil {
		t.Fatalf(err.Error())
	}

	os.Remove(TestingFile2)
	hf2, err := NewHeapFile(TestingFile2, &td2, bp)
	if err != nil {
		t.Fatalf(err.Error())
	}

	return hf1, hf2, t1, t2, bp
}

func TestJoinFieldOrder(t *testing.T) {
	bp, c, err := MakeTestDatabase(3, "join_test_catalog.txt")
	if err != nil {
		t.Fatalf(err.Error())
	}

	hf1, err := c.GetTable("test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	hf2, err := c.GetTable("test2")
	if err != nil {
		t.Fatalf(err.Error())
	}

	var t1 = Tuple{
		Desc: *hf1.Descriptor(),
		Fields: []DBValue{
			StringField{"sam"},
			IntField{25},
		}}

	var t2 = Tuple{
		Desc: *hf2.Descriptor(),
		Fields: []DBValue{
			StringField{"george jones"},
			IntField{25},
		}}

	tid := NewTID()
	bp.BeginTransaction(tid)

	insertTupleForTest(t, hf1, &t1, tid)
	insertTupleForTest(t, hf2, &t2, tid)

	leftField := FieldExpr{t1.Desc.Fields[1]}
	rightField := FieldExpr{t2.Desc.Fields[1]}

	join, err := NewJoin(hf1, &leftField, hf2, &rightField, 100)
	if err != nil {
		t.Errorf("unexpected error initializing join")
		return
	}
	iter, err := join.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("iter was nil")
	}

	var tdExpected = TupleDesc{Fields: []FieldType{
		{Fname: "a", Ftype: StringType},
		{Fname: "b", Ftype: IntType},
		{Fname: "c", Ftype: StringType},
		{Fname: "d", Ftype: IntType},
	}}

	tj, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if !tdExpected.equals(&tj.Desc) {
		t.Fatalf("Unexpected descriptor of joined tuple")
	}
}

func TestJoinTupleNil(t *testing.T) {
	_, t1, t2, _, _, _ := makeTestVars(t)
	tNew := joinTuples(&t1, nil)
	if !tNew.equals(&t1) {
		t.Fatalf("Unexpected output of joinTuple with nil")
	}
	tNew2 := joinTuples(nil, &t2)
	if !tNew2.equals(&t2) {
		t.Fatalf("Unexpected output of joinTuple with nil")
	}
}
