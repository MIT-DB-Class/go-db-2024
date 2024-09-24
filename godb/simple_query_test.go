package godb

import (
	"os"
	"testing"
)

func writeFile(t *testing.T, filename string, contents string) {
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, err = f.WriteString(contents)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSimpleQuery(t *testing.T) {
	bp, c, err := MakeParserTestDatabase(10000)
	if err != nil {
		t.Fatalf("failed to create test database, %s", err.Error())
	}

	hf1, err := c.GetTable("t")
	if err != nil {
		t.Fatalf("no table t, %s", err.Error())
	}
	hf2, err := c.GetTable("t2")
	if err != nil {
		t.Fatalf("no table t2, %s", err.Error())
	}

	f_name := FieldExpr{FieldType{"name", "", StringType}}
	joinOp, err := NewJoin(hf1, &f_name, hf2, &f_name, 1000)
	if err != nil {
		t.Fatalf("failed to construct join, %s", err.Error())
	}
	f_age := FieldExpr{FieldType{"age", "t", IntType}}
	e_const := ConstExpr{IntField{30}, IntType}
	filterOp, err := NewFilter(&e_const, OpGt, &f_age, joinOp)
	if err != nil {
		t.Fatalf("failed to construct filter, %s", err.Error())
	}
	if filterOp == nil {
		t.Fatalf("filter op was nil")
	}
	if filterOp.Descriptor() == nil {
		t.Fatalf("filter op descriptor was nil")
	}
	sa := CountAggState{}
	if len(filterOp.Descriptor().Fields) == 0 {
		t.Fatalf("filter op descriptor has no fields")
	}
	expr := FieldExpr{filterOp.Descriptor().Fields[0]}
	sa.Init("count", &expr)
	agg := NewAggregator([]AggState{&sa}, filterOp)
	tid := NewTID()
	bp.BeginTransaction(tid)
	f, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf("failed to get iterator, %s", err.Error())
	}
	tup, err := f()
	if err != nil {
		t.Fatalf("failed to get tuple, %s", err.Error())
	}
	cnt2 := tup.Fields[0].(IntField).Value
	if cnt2 != 10 {
		t.Fatalf("expected 10 results, got %d", cnt2)
	}
}
