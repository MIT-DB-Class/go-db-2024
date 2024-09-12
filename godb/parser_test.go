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

