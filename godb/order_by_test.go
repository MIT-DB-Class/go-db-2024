package godb

import (
	"os"
	"testing"
)

func makeOrderByOrderingVars() (DBFile, Tuple, TupleDesc, *BufferPool, error) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "a", Ftype: StringType},
		{Fname: "b", Ftype: IntType},
		{Fname: "c", Ftype: IntType},
	}}

	var t = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"sam"},
			IntField{25},
			IntField{5},
		}}

	bp, c, err := MakeTestDatabase(3, "catalog.txt")
	if err != nil {
		return nil, t, td, nil, err
	}

	os.Remove("test.dat")
	hf, err := c.addTable("test", td)
	if err != nil {
		return hf, t, td, nil, err
	}

	return hf, t, td, bp, nil
}

// test the order by operator, by asking it to sort the test database
// in ascending and descending order and verifying the result
func TestOrderBy(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	bs := make([]bool, 2)
	for i := range bs {
		bs[i] = false
	}
	//order by name and then age, descending
	exprs := make([]Expr, len(t1.Desc.Fields))
	for i, f := range t1.Desc.Fields {
		exprs[i] = &FieldExpr{f}
	}
	oby, err := NewOrderBy(exprs, hf, bs)
	if err != nil {
		t.Fatalf(err.Error())
	}

	iter, _ := oby.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	var last string
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		fval := tup.Fields[0].(StringField).Value
		if last != "" {
			if fval > last {
				t.Fatalf("data was not descending, as expected")
			}
		}
		last = fval
	}

	for i := range bs {
		bs[i] = true
	}
	//order by name and then age, ascending
	oby, err = NewOrderBy(exprs, hf, bs)
	if err != nil {
		t.Fatalf(err.Error())
	}

	iter, _ = oby.Iterator(tid)
	last = ""
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		fval := tup.Fields[0].(StringField).Value
		if last != "" {
			if fval < last {
				t.Fatalf("data was not ascending, as expected")
			}
		}
		last = fval
	}
}

// harder order by test that inserts 4 tuples, and alternates ascending vs descending
func TestOrderByMultiField(t *testing.T) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}

	var t1 = Tuple{
		Desc:   td,
		Fields: []DBValue{StringField{"sam"}, IntField{25}},
		Rid:    nil,
	}

	var t2 = Tuple{
		Desc:   td,
		Fields: []DBValue{StringField{"tim"}, IntField{44}},
		Rid:    nil,
	}

	var t3 = Tuple{
		Desc:   td,
		Fields: []DBValue{StringField{"mike"}, IntField{88}},
		Rid:    nil,
	}

	var t4 = Tuple{
		Desc:   td,
		Fields: []DBValue{StringField{"sam"}, IntField{26}},
		Rid:    nil,
	}

	bp, c, err := MakeTestDatabase(2, "catalog.txt")
	if err != nil {
		t.Fatalf(err.Error())
	}

	os.Remove("test.dat")
	hf, err := c.addTable("test", td)
	if err != nil {
		t.Fatalf(err.Error())
	}

	tid := NewTID()
	bp.BeginTransaction(tid)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	hf.insertTuple(&t3, tid)
	hf.insertTuple(&t4, tid)

	//order by name and then age, descending
	ascDescs := [][]bool{{false, false}, {true, false}}
	expectedAnswers := [][]Tuple{{t2, t4, t1, t3}, {t3, t4, t1, t2}}
	exprs := make([]Expr, len(t1.Desc.Fields))
	for i, f := range t1.Desc.Fields {
		exprs[i] = &FieldExpr{f}
	}

	for i := 0; i < len(ascDescs); i++ {
		ascDesc := ascDescs[i]
		expected := expectedAnswers[i]
		result := []Tuple{}
		oby, err := NewOrderBy(exprs, hf, ascDesc)
		if err != nil {
			t.Fatalf(err.Error())
		}
		iter, _ := oby.Iterator(tid)
		if iter == nil {
			t.Fatalf("iter was nil")
		}

		for {
			tup, _ := iter()
			if tup == nil {
				break
			}
			result = append(result, *tup)

		}
		if len(result) != len(expected) {
			t.Fatalf("order by test %d produced different number of results than expected (%d got, expected %d)", i, len(result), len(expected))
		}
		for j, tup := range result {
			if !tup.equals(&expected[j]) {
				t.Fatalf("order by test %d got wrong tuple at position %d (expected %v, got %v)", i, j, expected[j].Fields, tup.Fields)
			}
		}
	}

	bp.CommitTransaction(tid)
}

func TestOrderByFieldsOrder(t *testing.T) {
	hf, tup, td, bp, err := makeOrderByOrderingVars()
	if err != nil {
		t.Fatalf(err.Error())
	}

	tid := NewTID()
	bp.BeginTransaction(tid)
	hf.insertTuple(&tup, tid)

	bs := make([]bool, 2)
	for i := range bs {
		bs[i] = false
	}

	exprs := []Expr{&FieldExpr{td.Fields[0]}, &FieldExpr{td.Fields[2]}}

	oby, err := NewOrderBy(exprs, hf, bs)
	if err != nil {
		t.Fatalf(err.Error())
	}

	iter, _ := oby.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}

	var expectedDesc = TupleDesc{Fields: []FieldType{
		{Fname: "a", Ftype: StringType},
		{Fname: "b", Ftype: IntType},
		{Fname: "c", Ftype: IntType},
	}}

	tupOut, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if !expectedDesc.equals(&tupOut.Desc) {
		t.Fatalf("Unexpected descriptor of ordered tuple")
	}
}
