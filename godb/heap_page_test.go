package godb

import (
	"testing"
	"unsafe"
)

func TestInsertHeapPage(t *testing.T) {
	td, t1, t2, hf, _, _ := makeTestVars()
	pg, _ := newHeapPage(&td, 0, hf)
	var expectedSlots = (PageSize - 8) / (StringLength + int(unsafe.Sizeof(int64(0))))
	if pg.getNumSlots() != expectedSlots {
		t.Fatalf("Incorrect number of slots, expected %d, got %d", expectedSlots, pg.getNumSlots())
	}

	pg.insertTuple(&t1)
	pg.insertTuple(&t2)

	iter := pg.tupleIter()
	cnt := 0
	for {

		tup, _ := iter()
		if tup == nil {
			break
		}

		cnt += 1
	}
	if cnt != 2 {
		t.Errorf("Expected 2 tuples in interator, got %d", cnt)
	}
}

func TestDeleteHeapPage(t *testing.T) {
	td, t1, t2, hf, _, _ := makeTestVars()
	pg, _ := newHeapPage(&td, 0, hf)

	pg.insertTuple(&t1)
	slotNo, _ := pg.insertTuple(&t2)
	pg.deleteTuple(slotNo)

	iter := pg.tupleIter()
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	cnt := 0
	for {

		tup, _ := iter()
		if tup == nil {
			break
		}

		cnt += 1
	}
	if cnt != 1 {
		t.Errorf("Expected 1 tuple in interator, got %d", cnt)
	}
}

// Unit test for insertTuple
func TestHeapPageInsertTuple(t *testing.T) {
	td, t1, _, hf, _, _ := makeTestVars()
	page, _ := newHeapPage(&td, 0, hf)
	free := page.getNumSlots()

	for i := 0; i < free; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"sam"},
				IntField{int64(i)},
			},
		}
		page.insertTuple(&addition)

		iter := page.tupleIter()
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		cnt, found := 0, false
		for {

			tup, _ := iter()
			found = found || addition.equals(tup)
			if tup == nil {
				break
			}

			cnt += 1
		}
		if cnt != i+1 {
			t.Errorf("Expected %d tuple in interator, got %d", i+1, cnt)
		}
		if !found {
			t.Errorf("Expected inserted tuple to be FOUND, got NOT FOUND")
		}
	}

	_, err := page.insertTuple(&t1)

	if err == nil {
		t.Errorf("Expected error due to full page")
	}
}

// Unit test for deleteTuple
func TestHeapPageDeleteTuple(t *testing.T) {
	td, _, _, hf, _, _ := makeTestVars()
	page, _ := newHeapPage(&td, 0, hf)
	free := page.getNumSlots()

	list := make([]recordID, free)
	for i := 0; i < free; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"sam"},
				IntField{int64(i)},
			},
		}
		list[i], _ = page.insertTuple(&addition)
	}
	if len(list) == 0 {
		t.Fatalf("Rid list is empty.")
	}
	for i, rnd := free-1, 0xdefaced; i > 0; i, rnd = i-1, (rnd*0x7deface1+12354)%0x7deface9 {
		// Generate a random index j such that 0 <= j <= i.
		j := rnd % (i + 1)

		// Swap arr[i] and arr[j].
		list[i], list[j] = list[j], list[i]
	}

	for _, rid := range list {
		err := page.deleteTuple(rid)
		if err != nil {
			t.Errorf("Found error %s", err.Error())
		}
	}

	err := page.deleteTuple(list[0])
	if err == nil {
		t.Errorf("page should be empty; expected error")
	}
}

// Unit test for isDirty, setDirty
func TestHeapPageDirty(t *testing.T) {
	td, _, _, hf, _, tid := makeTestVars()
	page, _ := newHeapPage(&td, 0, hf)

	page.setDirty(tid, true)
	if !page.isDirty() {
		t.Errorf("page should be dirty")
	}
	page.setDirty(tid, true)
	if !page.isDirty() {
		t.Errorf("page should be dirty")
	}
	page.setDirty(tid, false)
	if page.isDirty() {
		t.Errorf("page should be not dirty")
	}
}

// Unit test for toBuffer and initFromBuffer
func TestHeapPageSerialization(t *testing.T) {

	td, _, _, hf, _, _ := makeTestVars()
	page, _ := newHeapPage(&td, 0, hf)
	free := page.getNumSlots()

	for i := 0; i < free-1; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"sam"},
				IntField{int64(i)},
			},
		}
		page.insertTuple(&addition)
	}

	buf, _ := page.toBuffer()
	page2, _ := newHeapPage(&td, 0, hf)
	err := page2.initFromBuffer(buf)
	if err != nil {
		t.Fatalf("Error loading heap page from buffer.")
	}

	iter, iter2 := page.tupleIter(), page2.tupleIter()
	if iter == nil {
		t.Fatalf("iter was nil.")
	}
	if iter2 == nil {
		t.Fatalf("iter2 was nil.")
	}

	findEqCount := func(t0 *Tuple, iter3 func() (*Tuple, error)) int {
		cnt := 0
		for tup, _ := iter3(); tup != nil; tup, _ = iter3() {
			if t0.equals(tup) {
				cnt += 1
			}
		}
		return cnt
	}

	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		if findEqCount(tup, page.tupleIter()) != findEqCount(tup, page2.tupleIter()) {
			t.Errorf("Serialization / deserialization doesn't result in identical heap page.")
		}
	}
}

func TestBufferLen(t *testing.T) {

	td, _, _, hf, _, _ := makeTestVars()
	page, _ := newHeapPage(&td, 0, hf)
	free := page.getNumSlots()

	for i := 0; i < free-1; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"sam"},
				IntField{int64(i)},
			},
		}
		page.insertTuple(&addition)
	}

	buf, _ := page.toBuffer()

	if buf.Len() != PageSize {
		t.Fatalf("HeapPage.toBuffer returns buffer of unexpected size;  NOTE:  This error may be OK, but many implementations that don't write full pages break.")
	}

}
