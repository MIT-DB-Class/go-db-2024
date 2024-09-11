package godb

import (
	"os"
	"testing"
)

const TestingFile string = "test.dat"
const TestingFile2 string = "test2.dat"

func makeTestVars() (TupleDesc, Tuple, Tuple, *HeapFile, *BufferPool, TransactionID) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}

	var t1 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"sam"},
			IntField{25},
		}}

	var t2 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"george jones"},
			IntField{999},
		}}

	bp, _ := NewBufferPool(3)
	os.Remove(TestingFile)
	hf, err := NewHeapFile(TestingFile, &td, bp)
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH")
		panic(err)
	}

	tid := NewTID()
	bp.BeginTransaction(tid)

	return td, t1, t2, hf, bp, tid

}

func TestCreateAndInsertHeapFile(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	iter, _ := hf.Iterator(tid)
	i := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		i = i + 1
	}
	if i != 2 {
		t.Errorf("HeapFile iterator expected 2 tuples, got %d", i)
	}
}

func TestDeleteHeapFile(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)

	hf.deleteTuple(&t1, tid)
	iter, _ := hf.Iterator(tid)
	t3, _ := iter()
	if t3 == nil {
		t.Errorf("HeapFile iterator expected 1 tuple")
	}
	hf.deleteTuple(&t2, tid)
	iter, _ = hf.Iterator(tid)
	t3, _ = iter()
	if t3 != nil {
		t.Errorf("HeapFile iterator expected 0 tuple")
	}
}

func testSerializeN(t *testing.T, n int) {
	td, t1, t2, hf, bp, _ := makeTestVars()
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

		// Force dirty pages to disk. CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2.
		bp.FlushAllPages()
		bp.CommitTransaction(tid)
	}
	bp.FlushAllPages()
	bp2, _ := NewBufferPool(1)
	hf2, _ := NewHeapFile(TestingFile, &td, bp2)
	tid := NewTID()
	bp2.BeginTransaction(tid)
	iter, _ := hf2.Iterator(tid)
	i := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		i = i + 1
	}
	if i != 2*n {
		t.Errorf("HeapFile iterator expected %d tuples, got %d", 2*n, i)
	}

}
func TestSerializeSmallHeapFile(t *testing.T) {
	testSerializeN(t, 2)
}

func TestSerializeLargeHeapFile(t *testing.T) {
	testSerializeN(t, 2000)
}

func TestSerializeVeryLargeHeapFile(t *testing.T) {
	testSerializeN(t, 4000)
}

func TestLoadCSV(t *testing.T) {
	_, _, _, hf, _, tid := makeTestVars()
	f, err := os.Open("test_heap_file.csv")
	if err != nil {
		t.Errorf("Couldn't open test_heap_file.csv")
		return
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
		t.Errorf("HeapFile iterator expected 384 tuples, got %d", i)
	}
}

func TestHeapFilePageKey(t *testing.T) {
	td, t1, _, hf, bp, tid := makeTestVars()

	os.Remove(TestingFile2)
	hf2, _ := NewHeapFile(TestingFile2, &td, bp)

	for hf.NumPages() < 2 {
		hf.insertTuple(&t1, tid)
		hf2.insertTuple(&t1, tid)
		if hf.NumPages() == 0 {
			t.Fatalf("Heap file should have at least one page after insertion.")
		}
	}

	if hf.NumPages() != hf2.NumPages() || hf.NumPages() != 2 {
		t.Fatalf("Should be two pages here")
	}

	for i := 0; i < hf.NumPages(); i++ {
		if hf.pageKey(i) != hf.pageKey(i) {
			t.Errorf("Expected equal pageKey")
		}
		if hf.pageKey(i) == hf.pageKey((i+1)%hf.NumPages()) {
			t.Errorf("Expected non-equal pageKey for different pages")
		}
		if hf.pageKey(i) == hf2.pageKey(i) {
			t.Errorf("Expected non-equal pageKey for different heapfiles")
		}
	}
}

func TestDirtyBit(t *testing.T) {
	_, t1, _, hf, bp, _ := makeTestVars()

	tid := NewTID()
	bp.BeginTransaction(tid)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t1, tid)
	page, _ := bp.GetPage(hf, 0, tid, ReadPerm)
	if !page.isDirty() {
		t.Fatalf("Expected page to be dirty")
	}
}

func TestHeapFileSize(t *testing.T) {
	_, t1, _, hf, bp, _ := makeTestVars()

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

func TestHeapFileIteratorExtra(t *testing.T) {
	_, t1, _, hf, bp, _ := makeTestVars()
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
