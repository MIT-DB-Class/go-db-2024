package godb

import (
	"os"
)

func MakeTestDatabase(bufferPoolSize int, catalog string) (*BufferPool, *Catalog, error) {
	bp, err := NewBufferPool(bufferPoolSize)
	if err != nil {
		return nil, nil, err
	}

	// load the catalog so we know which tables to remove
	c := NewCatalog(catalog, bp, "./")
	if err := c.parseCatalogFile(); err != nil {
		return nil, nil, err
	}
	for tableName := range c.tableMap {
		os.Remove(c.tableNameToFile(tableName))
	}

	// reload the catalog to reopen the table files
	c = NewCatalog(catalog, bp, "./")
	if err := c.parseCatalogFile(); err != nil {
		return nil, nil, err
	}
	return bp, c, nil
}

func MakeParserTestDatabase(bufferPoolSize int) (*BufferPool, *Catalog, error) {
	os.Remove("t2.dat")
	os.Remove("t.dat")

	bp, c, err := MakeTestDatabase(bufferPoolSize, "catalog.txt")
	if err != nil {
		return nil, nil, err
	}

	hf, err := c.GetTable("t")
	if err != nil {
		return nil, nil, err
	}
	hf2, err := c.GetTable("t2")
	if err != nil {
		return nil, nil, err
	}

	f, err := os.Open("testdb.txt")
	if err != nil {
		return nil, nil, err
	}
	err = hf.(*HeapFile).LoadFromCSV(f, true, ",", false)
	if err != nil {
		return nil, nil, err
	}

	f, err = os.Open("testdb.txt")
	if err != nil {
		return nil, nil, err
	}
	err = hf2.(*HeapFile).LoadFromCSV(f, true, ",", false)
	if err != nil {
		return nil, nil, err
	}

	if err := c.ComputeTableStats(); err != nil {
		return nil, nil, err
	}

	return bp, c, nil
}
