package godb

import (
	"testing"
)

func TestAggSimpleSum(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)

	err := hf.insertTuple(&t1, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.insertTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	sa := SumAggState{}
	expr := FieldExpr{t1.Desc.Fields[1]}
	err = sa.Init("sum", &expr)
	if err != nil {
		t.Fatalf(err.Error())
	}
	agg := NewAggregator([]AggState{&sa}, hf)
	iter, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	sum := tup.Fields[0].(IntField).Value
	if sum != 1024 {
		t.Errorf("unexpected sum")
	}
}

func TestAggMinStringAgg(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	err := hf.insertTuple(&t1, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.insertTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	sa := MinAggState{}
	expr := FieldExpr{t1.Desc.Fields[0]}
	err = sa.Init("min", &expr)
	if err != nil {
		t.Fatalf(err.Error())
	}
	agg := NewAggregator([]AggState{&sa}, hf)
	iter, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	min := tup.Fields[0].(StringField).Value
	if min != "george jones" {
		t.Errorf("incorrect min")
	}
}

func TestAggSimpleCount(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	err := hf.insertTuple(&t1, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.insertTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	sa := CountAggState{}
	expr := FieldExpr{t1.Desc.Fields[0]}
	err = sa.Init("count", &expr)
	if err != nil {
		t.Fatalf(err.Error())
	}
	agg := NewAggregator([]AggState{&sa}, hf)
	iter, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	cnt := tup.Fields[0].(IntField).Value
	if cnt != 2 {
		t.Errorf("unexpected count")
	}
}

func TestAggMulti(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	err := hf.insertTuple(&t1, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.insertTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	ca := CountAggState{}
	expr := FieldExpr{t1.Desc.Fields[0]}
	err = ca.Init("count", &expr)
	if err != nil {
		t.Fatalf(err.Error())
	}
	sa := SumAggState{}
	expr = FieldExpr{t1.Desc.Fields[1]}
	err = sa.Init("sum", &expr)
	if err != nil {
		t.Fatalf(err.Error())
	}
	agg := NewAggregator([]AggState{&ca, &sa}, hf)
	iter, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	cnt := tup.Fields[0].(IntField).Value
	if cnt != 2 {
		t.Errorf("unexpected count")
	}
	sum := tup.Fields[1].(IntField).Value
	if sum != 1024 {
		t.Errorf("unexpected sum")
	}
}

func TestAggGbyCount(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	err := hf.insertTuple(&t1, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.insertTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.insertTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.insertTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	gbyFields := []Expr{&FieldExpr{hf.Descriptor().Fields[0]}}
	sa := CountAggState{}
	expr := FieldExpr{t1.Desc.Fields[0]}
	err = sa.Init("count", &expr)
	if err != nil {
		t.Fatalf(err.Error())
	}

	agg := NewGroupedAggregator([]AggState{&sa}, gbyFields, hf)
	iter, _ := agg.Iterator(tid)
	fields := []FieldType{
		{"name", "", StringType},
		{"count", "", IntType},
	}
	outt1 := Tuple{TupleDesc{fields},
		[]DBValue{
			StringField{"sam"},
			IntField{1},
		},
		nil,
	}
	outt2 := Tuple{
		TupleDesc{fields},
		[]DBValue{
			StringField{"george jones"},
			IntField{3},
		},
		nil,
	}
	ts := []*Tuple{&outt1, &outt2}
	err = CheckIfOutputMatches(iter, ts)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestAggGbySum(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	err := hf.insertTuple(&t1, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.insertTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.insertTuple(&t1, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.insertTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	//gbyFields := hf.td.Fields[0:1]
	gbyFields := []Expr{&FieldExpr{hf.Descriptor().Fields[0]}}

	sa := SumAggState{}
	expr := FieldExpr{t1.Desc.Fields[1]}
	err = sa.Init("sum", &expr)
	if err != nil {
		t.Fatalf(err.Error())
	}

	agg := NewGroupedAggregator([]AggState{&sa}, gbyFields, hf)
	iter, _ := agg.Iterator(tid)

	fields := []FieldType{
		{"name", "", StringType},
		{"sum", "", IntType},
	}
	outt1 := Tuple{TupleDesc{fields},
		[]DBValue{
			StringField{"sam"},
			IntField{50},
		}, nil,
	}
	outt2 := Tuple{
		TupleDesc{fields},
		[]DBValue{
			StringField{"george jones"},
			IntField{1998},
		}, nil,
	}
	ts := []*Tuple{&outt1, &outt2}
	err = CheckIfOutputMatches(iter, ts)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestAggFilterCount(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	err := hf.insertTuple(&t1, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.insertTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	var f FieldType = FieldType{"age", "", IntType}
	filt, err := NewFilter(&ConstExpr{IntField{25}, IntType}, OpGt, &FieldExpr{f}, hf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if filt == nil {
		t.Fatalf("Filter returned nil")
	}

	sa := CountAggState{}
	expr := FieldExpr{t1.Desc.Fields[0]}
	err = sa.Init("count", &expr)
	if err != nil {
		t.Fatalf(err.Error())
	}
	agg := NewAggregator([]AggState{&sa}, filt)
	iter, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	cnt := tup.Fields[0].(IntField).Value
	if cnt != 1 {
		t.Errorf("unexpected count")
	}
}

func TestAggRepeatedIteration(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	err := hf.insertTuple(&t1, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.insertTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	sa := CountAggState{}
	expr := FieldExpr{t1.Desc.Fields[0]}
	err = sa.Init("count", &expr)
	if err != nil {
		t.Fatalf(err.Error())
	}
	agg := NewAggregator([]AggState{&sa}, hf)
	iter, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	cnt := tup.Fields[0].(IntField).Value
	if cnt != 2 {
		t.Errorf("unexpected count")
	}
	iter, err = agg.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	tup, err = iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if tup == nil {
		t.Fatalf("Expected non-null tuple")
	}
	cnt2 := tup.Fields[0].(IntField).Value
	if cnt != cnt2 {
		t.Errorf("count changed on repeated iteration")
	}
}
