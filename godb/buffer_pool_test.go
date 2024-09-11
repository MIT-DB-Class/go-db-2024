package godb

import (
	"os"
	"testing"
)

func TestBufferPoolGetPage(t *testing.T) {
	_, t1, t2, hf, bp, _ := makeTestVars(t)
	tid := NewTID()
	for i := 0; i < 300; i++ {
		bp.BeginTransaction(tid)
		err := hf.insertTuple(&t1, tid)
		if err != nil {
			t.Fatalf("%v", err)
		}
		err = hf.insertTuple(&t2, tid)
		if err != nil {
			t.Fatalf("%v", err)
		}

		// Force dirty pages to disk. CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2.
		bp.FlushAllPages()

		// commit transaction
		bp.CommitTransaction(tid)
	}
	bp.BeginTransaction(tid)
	//expect 6 pages
	for i := 0; i < 6; i++ {
		pg, err := bp.GetPage(hf, i, tid, ReadPerm)
		if pg == nil || err != nil {
			t.Fatalf("failed to get page %d (err = %v)", i, err)
		}
	}
	_, err := bp.GetPage(hf, 7, tid, ReadPerm)
	if err == nil {
		t.Fatalf("No error when getting page 7 from a file with 6 pages.")
	}
}

func TestSetDirty(t *testing.T) {
	_, t1, _, hf, bp, _ := makeTestVars(t)
	tid := NewTID()
	bp.BeginTransaction(tid)
	for i := 0; i < 308; i++ {
		err := hf.insertTuple(&t1, tid)
		if err != nil && (i == 306 || i == 307) {
			return
		} else if err != nil {
			t.Fatalf("%v", err)
		}
	}
	bp.CommitTransaction(tid)
	t.Fatalf("Expected error due to all pages in BufferPool being dirty")
}

// Test is only valid up to Lab 4. In Lab 5 we switch from FORCE/NOSTEAL to NOFORCE/STEAL.
func TestBufferPoolHoldsMultipleHeapFiles(t *testing.T) {
	if os.Getenv("LAB") == "5" {
		t.Skip("This test is only valid up to Lab 4. Skipping")
	}

	td, t1, t2, hf, bp, tid := makeTestVars(t)
	os.Remove(TestingFile2)
	hf2, err := NewHeapFile(TestingFile2, &td, bp)
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH")
		panic(err)
	}

	err1 := hf.insertTuple(&t1, tid)
	err2 := hf.insertTuple(&t1, tid)
	err3 := hf2.insertTuple(&t2, tid)

	if err1 != nil || err2 != nil || err3 != nil {
		t.Errorf("The BufferPool should be able to handle multiple files")
	}
	// bp contains 2 dirty pages at this point

	hf2TupCntPerPage := 0
	for hf2.NumPages() <= 1 {
		if err := hf2.insertTuple(&t2, tid); err != nil {
			t.Errorf("%v", err)
		}
		hf2TupCntPerPage++
	}
	// bp contains 3 dirty pages at this point

	for i := 0; i < hf2TupCntPerPage-1; i++ {
		if err := hf2.insertTuple(&t2, tid); err != nil {
			t.Errorf("%v", err)
		}
	}

	// bp contains 3 dirty pages at this point, including 2 full pages of hf2
	_ = hf2.insertTuple(&t2, tid)
	if err := hf2.insertTuple(&t2, tid); err == nil {
		t.Errorf("should cause bufferpool dirty page overflow here")
	}
}
