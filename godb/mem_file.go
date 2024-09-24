package godb

type MemPage struct {
	file  *MemFile
	tuple Tuple
}

type MemFile struct {
	fileNo int
	desc   *TupleDesc
	pages  []*MemPage
}

func (mp *MemPage) isDirty() bool {
	return false
}

func (mp *MemPage) setDirty(tid TransactionID, dirty bool) {
}

func (mp *MemPage) getFile() DBFile {
	return mp.file
}

func (mf *MemFile) NumPages() int {
	return len(mf.pages)
}

func (mf *MemFile) insertTuple(t *Tuple, tid TransactionID) error {
	for i := range mf.pages {
		if mf.pages[i] == nil {
			t.Rid = i
			mf.pages[i] = &MemPage{file: mf, tuple: *t}
		}
	}
	t.Rid = len(mf.pages)
	mf.pages = append(mf.pages, &MemPage{file: mf, tuple: *t})
	return nil
}

func (mf *MemFile) deleteTuple(t *Tuple, tid TransactionID) error {
	mf.pages[t.Rid.(int)] = nil
	return nil
}

func (mf *MemFile) readPage(pageNo int) (Page, error) {
	return mf.pages[pageNo], nil
}

func (mf *MemFile) flushPage(page Page) error {
	return nil
}

type MemPageKey struct {
	fileNo int
	pgNo   int
}

func (mf *MemFile) pageKey(pgNo int) any {
	return MemPageKey{mf.fileNo, pgNo}
}

func (mf *MemFile) Descriptor() *TupleDesc {
	return mf.desc
}

func (mf *MemFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	i := 0
	return func() (*Tuple, error) {
		for {
			if i >= len(mf.pages) {
				return nil, nil
			}
			page := mf.pages[i]
			if page == nil {
				i++
				continue
			}

			i++
			return &page.tuple, nil
		}
	}, nil
}

func CreateMemFileFromTuples(tuples []Tuple) *MemFile {
	desc := tuples[0].Desc
	file := &MemFile{desc: &desc, pages: make([]*MemPage, len(tuples))}
	for _, t := range tuples {
		file.insertTuple(&t, 0)
	}
	return file
}
