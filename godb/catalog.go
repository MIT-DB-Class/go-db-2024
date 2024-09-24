package godb

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

type Table struct {
	id   int
	name string
	desc TupleDesc

	// statistics
	stats *TableStats

	file DBFile
}

type Catalog struct {
	tableMap   map[string]*Table
	columnMap  map[string][]*Table
	bufferPool *BufferPool
	rootPath   string
	filePath   string
}

func (c *Catalog) SaveToFile(catalogFile string, rootPath string) error {
	f, err := os.OpenFile(rootPath+"/"+catalogFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	f.WriteString(c.String())
	f.Close()
	return nil
}

func (c *Catalog) dropTable(tableName string) error {
	_, ok := c.tableMap[tableName]
	if !ok {
		return GoDBError{NoSuchTableError, "couldn't find table to drop"}
	}

	delete(c.tableMap, tableName)
	for cn, ts := range c.columnMap {
		tsFiltered := make([]*Table, 0)
		for _, t := range ts {
			if t.name != tableName {
				tsFiltered = append(tsFiltered, t)
			}
		}
		c.columnMap[cn] = tsFiltered
	}
	return nil
}

func ImportCatalogFromCSVs(
	catalogFile string,
	bp *BufferPool,
	rootPath string,
	tableSuffix string,
	separator string) error {
	c, err := NewCatalogFromFile(catalogFile, bp, rootPath)
	if err != nil {
		return err
	}
	for _, t := range c.tableMap {
		fileName := rootPath + "/" + t.name + "." + tableSuffix
		log.Printf("Loading %s from %s...\n", t.name, fileName)
		hf, err := NewHeapFile(c.tableNameToFile(t.name), t.desc.copy(), c.bufferPool)
		if err != nil {
			return err
		}
		f, err := os.Open(fileName)
		if err != nil {
			return err
		}
		err = hf.LoadFromCSV(f, false, separator, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Catalog) parseCatalogFile() error {
	f, err := os.Open(c.rootPath + "/" + c.filePath)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		// code to read each line
		line := strings.ToLower(scanner.Text())
		sep := strings.Split(line, "(")
		if len(sep) != 2 {
			return GoDBError{ParseError, fmt.Sprintf("expected one paren in catalog entry, got %d (%s)", len(sep), line)}
		}
		tableName := strings.TrimSpace(sep[0])
		rest := strings.Trim(sep[1], "()")
		fields := strings.Split(rest, ",")

		var fieldArray []FieldType
		for _, f := range fields {
			f := strings.TrimSpace(f)
			nameType := strings.Split(f, " ")
			if len(nameType) < 2 || len(nameType) > 4 {
				return GoDBError{ParseError, fmt.Sprintf("malformed catalog entry %s (line %s)", nameType, line)}
			}

			name := nameType[0]
			fieldType := FieldType{name, "", IntType}
			switch nameType[1] {
			case "int":
				fallthrough
			case "integer":
				fieldType.Ftype = IntType
			case "string":
				fallthrough
			case "varchar":
				fallthrough
			case "text":
				fieldType.Ftype = StringType
			default:
				return GoDBError{ParseError, fmt.Sprintf("unknown type %s (line %s)", nameType[1], line)}
			}
			fieldArray = append(fieldArray, fieldType)
		}

		_, err := c.addTable(tableName, TupleDesc{fieldArray})
		if err != nil {
			return err
		}
	}
	return nil
}

func NewCatalog(catalogFile string, bp *BufferPool, rootPath string) *Catalog {
	return &Catalog{make(map[string]*Table), make(map[string][]*Table), bp, rootPath, catalogFile}
}

func NewCatalogFromFile(catalogFile string, bp *BufferPool, rootPath string) (*Catalog, error) {
	c := NewCatalog(catalogFile, bp, rootPath)
	if err := c.parseCatalogFile(); err != nil {
		return nil, err
	}
	return c, nil
}

// Add a new table to the catalog.
//
// Returns an error if the table already exists.
func (c *Catalog) addTable(named string, desc TupleDesc) (DBFile, error) {
	f, err := c.GetTable(named)
	if err == nil {
		return f, GoDBError{DuplicateTableError, fmt.Sprintf("a table named '%s' already exists", named)}
	}

	hf, err := NewHeapFile(c.tableNameToFile(named), &desc, c.bufferPool)
	if err != nil {
		return nil, err
	}

	t := &Table{len(c.tableMap), named, desc, nil, hf}
	c.tableMap[named] = t
	for _, f := range desc.Fields {
		mapList := c.columnMap[f.Fname]
		if mapList == nil {
			mapList = make([]*Table, 0)
		}
		c.columnMap[f.Fname] = append(mapList, t)
	}

	return hf, nil
}

func (c *Catalog) ComputeTableStats() error {
	// Dummy implementation, do not worry about it.
	return nil
}

func (c *Catalog) tableNameToFile(tableName string) string {
	return c.rootPath + "/" + tableName + ".dat"
}

func (c *Catalog) GetTableInfo(named string) (*Table, error) {
	t, ok := c.tableMap[named]
	if !ok {
		return nil, GoDBError{NoSuchTableError, fmt.Sprintf("no table '%s' found", named)}
	}
	return t, nil
}

func (c *Catalog) GetTable(named string) (DBFile, error) {
	t, err := c.GetTableInfo(named)
	if err != nil {
		return nil, err
	}
	return t.file, nil
}

func (c *Catalog) GetTableInfoId(id int) (*Table, error) {
	for _, t := range c.tableMap {
		if t.id == id {
			return t, nil
		}
	}
	return nil, GoDBError{NoSuchTableError, fmt.Sprintf("no table '%d' found", id)}
}

func (c *Catalog) GetTableInfoDBFile(f DBFile) (*Table, error) {
	for _, t := range c.tableMap {
		if t.file == f {
			return t, nil
		}
	}
	return nil, GoDBError{NoSuchTableError, "table not found"}
}

// Get the statistics for a table.
//
// Returns nil if the table does not exist.
func (c *Catalog) GetTableStats(named string) *TableStats {
	t, err := c.GetTableInfo(named)
	if err != nil {
		return nil
	}
	return t.stats
}

func (c *Catalog) findTablesWithColumn(named string) []*Table {
	return c.columnMap[named]
}

func (c *Catalog) NumTables() int {
	return len(c.tableMap)
}

func (t *Table) String() string {
	var buf strings.Builder
	buf.WriteString(t.name)
	buf.WriteByte('(')
	for i, f := range t.desc.Fields {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.Fname)
		buf.WriteByte(' ')
		buf.WriteString(f.Ftype.String())
	}
	buf.WriteString(")\n")
	return buf.String()
}

func (c *Catalog) String() string {
	var buf strings.Builder
	keys := make([]string, 0, len(c.tableMap))
	for k := range c.tableMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, t := range keys {
		buf.WriteString(c.tableMap[t].String())
	}
	return buf.String()
}

func (c *Catalog) CatalogString() string {
	return c.String()
}
