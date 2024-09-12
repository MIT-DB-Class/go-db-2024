package godb

import (
	"bytes"
	"fmt"
	"testing"
)

func CheckIfOutputMatches(f func() (*Tuple, error), ts []*Tuple) error {
	n := 0
	for {
		t1, err := f()
		if err != nil {
			return err
		}
		if t1 == nil {
			break
		}

		if n >= len(ts) {
			return fmt.Errorf("too many tuples returned. expected %d", len(ts))
		}

		t2 := ts[n]
		if !t1.equals(t2) {
			return fmt.Errorf("tuple %d did not match expected tuple. expected %v, got %v", n, t2, t1)
		}
		n++
	}
	if n < len(ts) {
		return fmt.Errorf("too few tuples returned. expected %d, got %d", len(ts), n)
	}
	return nil
}

func CheckIfOutputMatchesUnordered(f func() (*Tuple, error), ts []*Tuple) error {
	n := len(ts)
	found := make([]bool, n)

	i := 0
	for {
		t1, err := f()
		if err != nil {
			return err
		}
		if t1 == nil {
			break
		}

		if i >= n {
			return fmt.Errorf("too many tuples returned. expected %d", n)
		}

		found_this := false
		for j, t2 := range ts {
			if !found[j] && t1.equals(t2) {
				found[j] = true
				found_this = true
				break
			}
		}

		if !found_this {
			return fmt.Errorf("received unexpected tuple %v", t1)
		}
		i++
	}
	if i < n {
		return fmt.Errorf("too few tuples returned. expected %d, got %d", n, i)
	}
	for j, f := range found {
		if !f {
			return fmt.Errorf("missing tuple %v", ts[j])
		}
	}
	return nil
}

func makeTupleTestVars() (TupleDesc, Tuple, Tuple) {
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

	return td, t1, t2
}

// Unit test for Tuple.writeTo() and Tuple.readTupleFrom()
func TestTupleSerialization(t *testing.T) {
	td, t1, _ := makeTupleTestVars()
	b := new(bytes.Buffer)
	t1.writeTo(b)
	t3, err := readTupleFrom(b, &td)
	if err != nil {
		t.Fatalf("Error loading tuple from saved buffer: %v", err.Error())
	}
	if !t3.equals(&t1) {
		t.Errorf("Serialization / deserialization doesn't result in identical tuple.")
	}
}

// Unit test for Tuple.compareField()
func TestTupleExpr(t *testing.T) {
	td, t1, t2 := makeTupleTestVars()
	ft := td.Fields[0]
	f := FieldExpr{ft}
	result, err := t1.compareField(&t2, &f) // compare "sam" to "george jones"
	if err != nil {
		t.Fatalf(err.Error())
	}
	if result != OrderedGreaterThan {
		t.Errorf("comparison of fields did not return expected result")
	}
}

// Unit test for Tuple.project()
func TestTupleProject(t *testing.T) {
	_, t1, _ := makeTupleTestVars()
	tNew, err := t1.project([]FieldType{t1.Desc.Fields[0]})
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tNew == nil {
		t.Fatalf("new tuple was nil")
	}
	if len(tNew.Fields) != 1 {
		t.Fatalf("unexpected number of fields after project")
	}
	f, ok := tNew.Fields[0].(StringField)
	if !ok || f.Value != "sam" {
		t.Errorf("unexpected value after project")
	}
}

// Unit test for Tuple.project()
func TestTupleProjectQualifier(t *testing.T) {
	td1 := TupleDesc{Fields: []FieldType{{Fname: "f", TableQualifier: "t1", Ftype: IntType}, {Fname: "f", TableQualifier: "t2", Ftype: IntType}}}
	t1 := Tuple{td1, []DBValue{IntField{1}, IntField{2}}, nil}

	tNew, err := t1.project([]FieldType{t1.Desc.Fields[1]})
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tNew == nil {
		t.Fatalf("new tuple was nil")
	}
	if len(tNew.Fields) != 1 {
		t.Fatalf("unexpected number of fields after project")
	}
	f, ok := tNew.Fields[0].(IntField)
	if !ok || f.Value != 2 {
		t.Errorf("failed to select t2.f")
	}

	td2 := TupleDesc{Fields: []FieldType{{Fname: "g", TableQualifier: "t1", Ftype: IntType}, {Fname: "f", TableQualifier: "t2", Ftype: IntType}}}
	t2 := Tuple{td2, []DBValue{IntField{1}, IntField{2}}, nil}

	tNew, err = t2.project([]FieldType{{Fname: "f", TableQualifier: "t1", Ftype: IntType}})
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tNew == nil {
		t.Fatalf("new tuple was nil")
	}
	if len(tNew.Fields) != 1 {
		t.Fatalf("unexpected number of fields after project")
	}
	f, ok = tNew.Fields[0].(IntField)
	if !ok || f.Value != 2 {
		t.Errorf("failed to select t2.f")
	}
}

// Unit test for Tuple.joinTuples()
func TestTupleJoin(t *testing.T) {
	_, t1, t2 := makeTupleTestVars()
	tNew := joinTuples(&t1, &t2)
	if len(tNew.Fields) != 4 {
		t.Fatalf("unexpected number of fields after join")
	}
	if len(tNew.Desc.Fields) != 4 {
		t.Fatalf("unexpected number of fields in description after join")
	}
	f, ok := tNew.Fields[0].(StringField)
	if !ok || f.Value != "sam" {
		t.Fatalf("unexpected value after join")
	}
	f, ok = tNew.Fields[2].(StringField)
	if !ok || f.Value != "george jones" {
		t.Errorf("unexpected value after join")
	}

}

func TDAssertEquals(t *testing.T, expected, actual TupleDesc) {
	if !(expected.equals(&actual)) {
		t.Errorf("Expected EQUAL, found NOT EQUAL")
	}
}

func TDAssertNotEquals(t *testing.T, expected, actual TupleDesc) {
	if expected.equals(&actual) {
		t.Errorf("Expected EQUAL, found NOT EQUAL")
	}
}

func TAssertEquals(t *testing.T, expected, actual Tuple) {
	if !(expected.equals(&actual)) {
		t.Errorf("Expected EQUAL, found NOT EQUAL")
	}
}

func TAssertNotEquals(t *testing.T, expected, actual Tuple) {
	if expected.equals(&actual) {
		t.Errorf("Expected NOT EQUAL, found EQUAL")
	}
}

func TestTupleDescEquals(t *testing.T) {
	singleInt := TupleDesc{Fields: []FieldType{{Ftype: IntType}}}
	singleInt2 := TupleDesc{Fields: []FieldType{{Ftype: IntType}}}
	intString := TupleDesc{Fields: []FieldType{{Ftype: IntType}, {Ftype: StringType}}}
	intString2 := TupleDesc{Fields: []FieldType{{Ftype: IntType}, {Ftype: StringType}}}

	TDAssertEquals(t, singleInt, singleInt)
	TDAssertEquals(t, singleInt, singleInt2)
	TDAssertEquals(t, singleInt2, singleInt)
	TDAssertEquals(t, intString, intString)

	TDAssertNotEquals(t, singleInt, intString)
	TDAssertNotEquals(t, singleInt2, intString)
	TDAssertNotEquals(t, intString, singleInt)
	TDAssertNotEquals(t, intString, singleInt2)
	TDAssertEquals(t, intString, intString2)
	TDAssertEquals(t, intString2, intString)

	stringInt := TupleDesc{Fields: []FieldType{{Ftype: StringType}, {Ftype: IntType}}}
	_, t1, _ := makeTupleTestVars()
	TDAssertNotEquals(t, t1.Desc, stringInt) // diff in only Fname
}

// Unit test for TupleDesc.copy()
func TestTupleDescCopy(t *testing.T) {
	singleInt := TupleDesc{Fields: []FieldType{{Ftype: IntType}}}
	intString := TupleDesc{Fields: []FieldType{{Ftype: IntType}, {Ftype: StringType}}}

	TDAssertEquals(t, singleInt, *singleInt.copy())
	TDAssertEquals(t, intString, *intString.copy())
	TDAssertEquals(t, *intString.copy(), *intString.copy())
	TDAssertNotEquals(t, *intString.copy(), *singleInt.copy())

	// tests deep copy
	tdCpy := intString.copy()
	tdCpy2 := tdCpy.copy()
	if tdCpy == nil || len(tdCpy.Fields) == 0 {
		t.Fatalf("tdCpy is nil or fields are empty")
	}
	if tdCpy2 == nil || len(tdCpy2.Fields) == 0 {
		t.Fatalf("tdCpy2 is nil or fields are empty")
	}
	tdCpy.Fields[0] = intString.Fields[1]
	TDAssertNotEquals(t, *tdCpy, *tdCpy2)
	tdCpy.Fields[0] = intString.Fields[0]
	TDAssertEquals(t, *tdCpy, *tdCpy2)
}

// Unit test for TupleDesc.merge()
func TestTupleDescMerge(t *testing.T) {
	singleInt := TupleDesc{Fields: []FieldType{{Ftype: IntType}}}
	stringInt := TupleDesc{Fields: []FieldType{{Ftype: StringType}, {Ftype: IntType}}}
	td1, td2 := stringInt, stringInt.copy()

	tdNew := td1.merge(&singleInt).merge(td2)
	final := TupleDesc{Fields: []FieldType{{Ftype: StringType}, {Ftype: IntType}, {Ftype: IntType}, {Ftype: StringType}, {Ftype: IntType}}}

	TDAssertEquals(t, final, *tdNew)
	TDAssertNotEquals(t, td1, *tdNew)
}

// Unit test for Tuple.equals()
func TestTupleEquals(t *testing.T) {
	_, t1, t2 := makeTupleTestVars()
	_, t1Dup, _ := makeTupleTestVars()

	var stringTup = Tuple{
		Desc: TupleDesc{Fields: []FieldType{{Ftype: StringType}}},
		Fields: []DBValue{
			StringField{"sam"},
		},
	}

	TAssertEquals(t, t1, t1)
	TAssertEquals(t, t1, t1Dup)

	TAssertNotEquals(t, t1, t2)
	TAssertNotEquals(t, t1, stringTup)
	TAssertNotEquals(t, stringTup, t2)
}

func TestJoinTuplesDesc(t *testing.T) {
	_, t1, t2 := makeTupleTestVars()
	tNew := joinTuples(&t1, &t2)
	if len(tNew.Desc.Fields) != 4 {
		t.Fatalf("Expected 4 fields in desc after join")
	}
	fields := []string{"name", "age", "name", "age"}
	for i, fname := range fields {
		if tNew.Desc.Fields[i].Fname != fname {
			t.Fatalf("expected %dth field to be named %s", i, fname)
		}
	}
}

func TestTupleJoinDesc(t *testing.T) {
	var td1 = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}

	var td2 = TupleDesc{Fields: []FieldType{
		{Fname: "age2", Ftype: IntType},
		{Fname: "name2", Ftype: StringType},
	}}

	var t1 = Tuple{
		Desc: td1,
		Fields: []DBValue{
			StringField{"sam"},
			IntField{25},
		}}

	var t2 = Tuple{
		Desc: td2,
		Fields: []DBValue{
			IntField{999},
			StringField{"george jones"},
		}}

	tNew := joinTuples(&t1, &t2)
	if len(tNew.Desc.Fields) != 4 {
		t.Fatalf("unexpected number of desc fields after join")
	}

	var tdAns = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
		{Fname: "age2", Ftype: IntType},
		{Fname: "name2", Ftype: StringType},
	}}

	if !tNew.Desc.equals(&tdAns) {
		t.Fatalf("unexpected desc after join")
	}
}

func TestTupleProject2(t *testing.T) {
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
		t.Fatalf(err.Error())
	}

	if t2.Fields[0].(StringField).Value != "SFname1tq1" {
		t.Errorf("wrong match 0")
	}
	if t2.Fields[1].(StringField).Value != "SFname2tq2" {
		t.Errorf("wrong match 1")
	}
	if t2.Fields[2].(StringField).Value != "SFname1tq1" {
		t.Errorf("wrong match 2")
	}
	if t2.Fields[3].(StringField).Value != "SFname2tq2" {
		t.Errorf("wrong match 3")
	}
	if t2.Fields[4].(StringField).Value != "SFname1tq2" {
		t.Errorf("wrong match 4")
	}
}

func TestTupleProject3(t *testing.T) {
	td1 := TupleDesc{Fields: []FieldType{
		{Fname: "a", Ftype: StringType},
		{Fname: "b", Ftype: IntType},
	}}

	t1 := Tuple{
		Desc: td1,
		Fields: []DBValue{
			StringField{"sam"},
			IntField{25},
		}}

	ft1 := FieldType{"a", "", StringType}
	ft2 := FieldType{"b", "", IntType}
	outTup, err := t1.project([]FieldType{ft1})
	if err != nil {
		t.Fatalf(err.Error())
	}
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

func TestTupleJoinNil(t *testing.T) {
	_, t1, t2 := makeTupleTestVars()
	tNew := joinTuples(&t1, nil)
	if !tNew.equals(&t1) {
		t.Fatalf("Unexpected output of joinTuple with nil")
	}
	if tNew.equals(&t2) {
		t.Fatalf("Unexpected output of joinTuple with nil")
	}
	tNew2 := joinTuples(nil, &t2)
	if !tNew2.equals(&t2) {
		t.Fatalf("Unexpected output of joinTuple with nil")
	}
	if tNew2.equals(&t1) {
		t.Fatalf("Unexpected output of joinTuple with nil")
	}
}

func TestTupleJoinDesc2(t *testing.T) {
	_, t1, t2 := makeTupleTestVars()
	tNew := joinTuples(&t1, &t2)
	if len(tNew.Desc.Fields) != 4 {
		t.Fatalf("Expected 4 fields in desc after join")
	}
	fields := []string{"name", "age", "name", "age"}
	for i, fname := range fields {
		if tNew.Desc.Fields[i].Fname != fname {
			t.Fatalf("expected %dth field to be named %s", i, fname)
		}
	}
}
