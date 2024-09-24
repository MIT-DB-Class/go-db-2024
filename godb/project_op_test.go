package godb

import (
	"testing"
)

func TestProject(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	//fs := make([]FieldType, 1)
	//fs[0] = t1.Desc.Fields[0]
	var outNames []string = make([]string, 1)
	outNames[0] = "outf"
	fieldExpr := FieldExpr{t1.Desc.Fields[0]}
	proj, _ := NewProjectOp([]Expr{&fieldExpr}, outNames, false, hf)
	if proj == nil {
		t.Fatalf("project was nil")
	}
	iter, _ := proj.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if len(tup.Fields) != 1 || tup.Desc.Fields[0].Fname != "outf" {
		t.Errorf("invalid output tuple")
	}

}

func TestProjectDistinctOptional(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)

	//fs := make([]FieldType, 1)
	//fs[0] = t1.Desc.Fields[0]
	var outNames []string = make([]string, 1)
	outNames[0] = "outf"
	fieldExpr := FieldExpr{t1.Desc.Fields[0]}
	proj, _ := NewProjectOp([]Expr{&fieldExpr}, outNames, true, hf)
	if proj == nil {
		t.Fatalf("project was nil")
	}
	iter, _ := proj.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	cnt := 0
	for {
		tup, err := iter()
		if err != nil {
			t.Fatalf(err.Error())
		}
		if tup == nil {
			break
		}
		cnt = cnt + 1
	}
	if cnt != 2 {
		t.Errorf("expected two names, got %d", cnt)

	}
}

func TestProjectOrdering(t *testing.T) {
	hf, tup, td, bp, err := makeOrderByOrderingVars()
	if err != nil {
		t.Fatalf(err.Error())
	}

	tid := NewTID()
	bp.BeginTransaction(tid)
	hf.insertTuple(&tup, tid)

	var outNames = []string{"out1", "out2"}
	exprs := []Expr{&FieldExpr{td.Fields[2]}, &FieldExpr{td.Fields[0]}}

	proj, _ := NewProjectOp(exprs, outNames, false, hf)
	if proj == nil {
		t.Fatalf("project was nil")
	}
	iter, _ := proj.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}

	tupOut, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}

	var expectedDesc = TupleDesc{Fields: []FieldType{
		{Fname: "out1", Ftype: IntType},
		{Fname: "out2", Ftype: StringType},
	}}

	if !expectedDesc.equals(&tupOut.Desc) {
		t.Fatalf("Unexpected descriptor of projected tuple")
	}

}

func TestProjectExtra(t *testing.T) {
	_, _, t1, _, _ := makeJoinOrderingVars(t)
	ft1 := FieldType{"a", "", StringType}
	ft2 := FieldType{"b", "", IntType}
	outTup, _ := t1.project([]FieldType{ft1})
	if (len(outTup.Fields)) != 1 {
		t.Fatalf("project returned %d fields, expected 1", len(outTup.Fields))
	}
	v, ok := outTup.Fields[0].(StringField)

	if !ok {
		t.Fatalf("project of name didn't return string")
	}
	if v.Value != "sam" {
		t.Fatalf("project didn't return sam")

	}
	outTup, _ = t1.project([]FieldType{ft2})
	if (len(outTup.Fields)) != 1 {
		t.Fatalf("project returned %d fields, expected 1", len(outTup.Fields))
	}
	v2, ok := outTup.Fields[0].(IntField)

	if !ok {
		t.Fatalf("project of name didn't return int")
	}
	if v2.Value != 25 {
		t.Fatalf("project didn't return 25")

	}

	outTup, _ = t1.project([]FieldType{ft2, ft1})
	if (len(outTup.Fields)) != 2 {
		t.Fatalf("project returned %d fields, expected 2", len(outTup.Fields))
	}
	v, ok = outTup.Fields[1].(StringField)
	if !ok {
		t.Fatalf("project of name didn't return string in second field")
	}
	if v.Value != "sam" {
		t.Fatalf("project didn't return sam")

	}

	v2, ok = outTup.Fields[0].(IntField)
	if !ok {
		t.Fatalf("project of name didn't return int in first field")
	}
	if v2.Value != 25 {
		t.Fatalf("project didn't return 25")

	}

}

func TestTupleProjectExtra(t *testing.T) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name1", TableQualifier: "tq1", Ftype: StringType},
		{Fname: "name2", TableQualifier: "tq2", Ftype: StringType},
		{Fname: "name1", TableQualifier: "tq2", Ftype: StringType},
	}}

	var t1 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"SFname1tq1"},
			StringField{"SFname2tq2"},
			StringField{"SFname1tq2"},
		}}

	t2, err := t1.project([]FieldType{
		{Fname: "name1", TableQualifier: "tq1", Ftype: StringType},
		{Fname: "name2", TableQualifier: "", Ftype: StringType},
		{Fname: "name1", TableQualifier: "tq1", Ftype: StringType},
		{Fname: "name2", TableQualifier: "tq2", Ftype: StringType},
		{Fname: "name1", TableQualifier: "tq2", Ftype: StringType},
	})

	if err != nil {
		t.Fatalf("%v", err)
	}

	if t2.Fields[0].(StringField).Value != "SFname1tq1" {
		t.Fatalf("tuple project extra wrong match")
	}

	if t2.Fields[1].(StringField).Value != "SFname2tq2" {
		t.Fatalf("tuple project extra wrong match")
	}

	if t2.Fields[2].(StringField).Value != "SFname1tq1" {
		t.Fatalf("tuple project extra wrong match")
	}
	if t2.Fields[3].(StringField).Value != "SFname2tq2" {
		t.Fatalf("tuple project extra wrong match")
	}
	if t2.Fields[4].(StringField).Value != "SFname1tq2" {
		t.Fatalf("tuple project extra wrong match")
	}

}
