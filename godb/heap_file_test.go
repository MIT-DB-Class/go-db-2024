package godb

import (
	"os"
	"testing"
)

const TestingFile string = "test.dat"
const TestingFile2 string = "test2.dat"

func makeTestFile(t *testing.T, bufferPoolSize int) (*BufferPool, *HeapFile) {
	os.Remove(TestingFile)

	bp, c, err := MakeTestDatabase(bufferPoolSize, "catalog.txt")
	if err != nil {
		t.Fatalf(err.Error())
	}

	td, _, _ := makeTupleTestVars()
	tbl, err := c.addTable("test", td)
	if err != nil {
		t.Fatalf(err.Error())
	}
	return bp, tbl.(*HeapFile)
}

func makeTestVars(t *testing.T) (TupleDesc, Tuple, Tuple, *HeapFile, *BufferPool, TransactionID) {
	bp, hf := makeTestFile(t, 3)
	td, t1, t2 := makeTupleTestVars()
	tid := NewTID()
	bp.BeginTransaction(tid)
	return td, t1, t2, hf, bp, tid
}

func TestHeapFileCreateAndInsert(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	err := hf.insertTuple(&t1, tid)

	hf.insertTuple(&t2, tid)
	iter, err := hf.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	i := 0
	for {
		tup, err := iter()
		if err != nil {
			t.Fatalf(err.Error())
		}

		if tup == nil {
			break
		}
		i = i + 1
	}
	if i != 2 {
		t.Fatalf("HeapFile iterator expected 2 tuples, got %d", i)
	}
}

func TestHeapFileDelete(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	err := hf.insertTuple(&t1, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = hf.insertTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = hf.deleteTuple(&t1, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	iter, err := hf.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	t3, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if t3 == nil {
		t.Fatalf("HeapFile iterator expected 1 tuple")
	}

	err = hf.deleteTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	iter, err = hf.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	t3, err = iter()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if t3 != nil {
		t.Fatalf("HeapFile iterator expected 0 tuple")
	}
}

func testSerializeN(t *testing.T, n int) {
	bp, hf := makeTestFile(t, max(1, n/50))
	_, t1, t2 := makeTupleTestVars()

	tid := NewTID()
	bp.BeginTransaction(tid)
	for i := 0; i < n; i++ {
		if err := hf.insertTuple(&t1, tid); err != nil {
			t.Fatalf(err.Error())
		}

		if err := hf.insertTuple(&t2, tid); err != nil {
			t.Fatalf(err.Error())
		}
	}
	bp.CommitTransaction(tid)
	bp.FlushAllPages()

	bp2, catalog, err := MakeTestDatabase(1, "catalog.txt")
	if err != nil {
		t.Fatalf(err.Error())
	}
	hf2, err := catalog.addTable("test", *hf.Descriptor())
	if err != nil {
		t.Fatalf(err.Error())
	}

	tid = NewTID()
	bp2.BeginTransaction(tid)

	iter, err := hf2.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	i := 0
	for tup, err := iter(); tup != nil; tup, err = iter() {
		if err != nil {
			t.Fatalf(err.Error())
		}
		i = i + 1
	}
	if i != 2*n {
		t.Fatalf("HeapFile iterator expected %d tuples, got %d", 2*n, i)
	}

}
func TestHeapFileSerializeSmall(t *testing.T) {
	testSerializeN(t, 2)
}

func TestHeapFileSerializeLarge(t *testing.T) {
	testSerializeN(t, 2000)
}

func TestHeapFileSerializeVeryLarge(t *testing.T) {
	testSerializeN(t, 4000)
}

func TestHeapFileLoadCSV(t *testing.T) {
	_, _, _, hf, _, tid := makeTestVars(t)
	f, err := os.Open("test_heap_file.csv")
	if err != nil {
		t.Fatalf("Couldn't open test_heap_file.csv")
	}
	err = hf.LoadFromCSV(f, true, ",", false)
	if err != nil {
		t.Fatalf("Load failed, %s", err)
	}
	//should have 384 records
	iter, _ := hf.Iterator(tid)
	i := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		i = i + 1
	}
	if i != 384 {
		t.Fatalf("HeapFile iterator expected 384 tuples, got %d", i)
	}
}

func TestHeapFilePageKey(t *testing.T) {
	td, t1, _, hf, bp, tid := makeTestVars(t)

	os.Remove(TestingFile2)
	hf2, err := NewHeapFile(TestingFile2, &td, bp)
	if err != nil {
		t.Fatalf(err.Error())
	}

	for hf.NumPages() < 2 {
		err = hf.insertTuple(&t1, tid)
		if err != nil {
			t.Fatalf(err.Error())
		}

		err = hf2.insertTuple(&t1, tid)
		if err != nil {
			t.Fatalf(err.Error())
		}

		if hf.NumPages() == 0 {
			t.Fatalf("Heap file should have at least one page after insertion.")
		}

		bp.FlushAllPages()
	}

	if hf.NumPages() != hf2.NumPages() || hf.NumPages() != 2 {
		t.Fatalf("Should be two pages here")
	}

	for i := 0; i < hf.NumPages(); i++ {
		if hf.pageKey(i) != hf.pageKey(i) {
			t.Fatalf("Expected equal pageKey")
		}
		if hf.pageKey(i) == hf.pageKey((i+1)%hf.NumPages()) {
			t.Fatalf("Expected non-equal pageKey for different pages")
		}
		if hf.pageKey(i) == hf2.pageKey(i) {
			t.Fatalf("Expected non-equal pageKey for different heapfiles")
		}
	}
}

func TestHeapFileSize(t *testing.T) {
	_, t1, _, hf, bp, _ := makeTestVars(t)

	tid := NewTID()
	bp.BeginTransaction(tid)
	hf.insertTuple(&t1, tid)
	page, err := bp.GetPage(hf, 0, tid, ReadPerm)
	if err != nil {
		t.Fatalf("unexpected error, getPage, %s", err.Error())
	}
	hf.flushPage(page)
	info, err := os.Stat(TestingFile)
	if err != nil {
		t.Fatalf("unexpected error, stat, %s", err.Error())
	}
	if info.Size() != int64(PageSize) {
		t.Fatalf("heap file page is not %d bytes;  NOTE:  This error may be OK, but many implementations that don't write full pages break.", PageSize)
	}
}

func TestHeapFileSetDirty(t *testing.T) {
	if os.Getenv("LAB") == "5" {
		t.Skip("This test is only valid up to Lab 4. Skipping")
	}

	_, t1, _, hf, bp, tid := makeTestVars(t)
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

func TestHeapFileDirtyBit(t *testing.T) {
	_, t1, _, hf, bp, _ := makeTestVars(t)

	tid := NewTID()
	bp.BeginTransaction(tid)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t1, tid)
	page, _ := bp.GetPage(hf, 0, tid, ReadPerm)
	if !page.isDirty() {
		t.Fatalf("Expected page to be dirty")
	}
}

func TestHeapFileIteratorExtra(t *testing.T) {
	_, t1, _, hf, bp, _ := makeTestVars(t)
	tid := NewTID()
	bp.BeginTransaction(tid)

	it, err := hf.Iterator(tid)
	_, err = it()
	if err != nil {
		t.Fatalf("Empty heap file iterator should return nil,nil")
	}
	hf.insertTuple(&t1, tid)
	it, err = hf.Iterator(tid)
	pg, err := it()
	if err != nil {
		t.Fatalf("Iterating over heap file with one tuple returned error %s", err.Error())
	}
	if pg == nil {
		t.Fatalf("Should have gotten 1 page in heap file iterator")
	}
	pg, err = it()
	if pg != nil {
		t.Fatalf("More than 1 page in heap file iterator!")
	}
	if err != nil {
		t.Fatalf("Iterator returned error at end, expected nil, nil, got nil, %s", err.Error())
	}
}
