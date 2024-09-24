package godb

import (
"fmt"
)

// interface for an aggregation state
type AggState interface {
	// Initializes an aggregation state. Is supplied with an alias, an expr to
	// evaluate an input tuple into a DBValue, and a getter to extract from the
	// DBValue its int or string field's value.
	Init(alias string, expr Expr) error

	// Makes an copy of the aggregation state.
	Copy() AggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// Implements the aggregation state for COUNT
// We are supplying the implementation of CountAggState as an example. You need to
// implement the rest of the aggregation states.
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *CountAggState) Init(alias string, expr Expr) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Implements the aggregation state for SUM
type SumAggState struct {
	// TODO: some code goes here
}

func (a *SumAggState) Copy() AggState {
	// TODO: some code goes here
	return nil // replace me
}

func intAggGetter(v DBValue) any {
	// TODO: some code goes here
	return nil // replace me
}

func stringAggGetter(v DBValue) any {
	// TODO: some code goes here
	return nil // replace me
}

func (a *SumAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	return fmt.Errorf("SumAggState.Init not implemented") // replace me
}

func (a *SumAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
}

func (a *SumAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	return &TupleDesc{} // replace me
}

func (a *SumAggState) Finalize() *Tuple {
	// TODO: some code goes here
	return &Tuple{} // replace me
}

// Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState struct {
	// TODO: some code goes here
}

func (a *AvgAggState) Copy() AggState {
	// TODO: some code goes here
	return nil // replace me
}

func (a *AvgAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	return fmt.Errorf("AvgAggState.Init not implemented") // replace me
}

func (a *AvgAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
}

func (a *AvgAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	return &TupleDesc{} // replace me
}

func (a *AvgAggState) Finalize() *Tuple {
	// TODO: some code goes here
	return &Tuple{} // replace me
}

// Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState struct {
	// TODO: some code goes here
}

func (a *MaxAggState) Copy() AggState {
	// TODO: some code goes here
	return nil // replace me
}

func (a *MaxAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	return fmt.Errorf("MaxAggState.Init not implemented") // replace me
}

func (a *MaxAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
}

func (a *MaxAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	return &TupleDesc{} // replace me
}

func (a *MaxAggState) Finalize() *Tuple {
	// TODO: some code goes here
	return &Tuple{} // replace me
}

// Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState struct {
	// TODO: some code goes here
}

func (a *MinAggState) Copy() AggState {
	// TODO: some code goes here
	return nil // replace me
}

func (a *MinAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	return fmt.Errorf("MinAggState.Init not implemented") // replace me
}

func (a *MinAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
}

func (a *MinAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	return &TupleDesc{} // replace me
}

func (a *MinAggState) Finalize() *Tuple {
	// TODO: some code goes here
	return &Tuple{} // replace me
}
