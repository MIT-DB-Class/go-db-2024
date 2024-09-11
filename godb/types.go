package godb

import (
	"fmt"
	"regexp"
	"strings"
)

type GoDBErrorCode int

const (
	TupleNotFoundError      GoDBErrorCode = iota
	PageFullError           GoDBErrorCode = iota
	IncompatibleTypesError  GoDBErrorCode = iota
	TypeMismatchError       GoDBErrorCode = iota
	MalformedDataError      GoDBErrorCode = iota
	BufferPoolFullError     GoDBErrorCode = iota
	ParseError              GoDBErrorCode = iota
	DuplicateTableError     GoDBErrorCode = iota
	NoSuchTableError        GoDBErrorCode = iota
	AmbiguousNameError      GoDBErrorCode = iota
	IllegalOperationError   GoDBErrorCode = iota
	DeadlockError           GoDBErrorCode = iota
	IllegalTransactionError GoDBErrorCode = iota
)

//go:generate stringer -type=GoDBErrorCode

type GoDBError struct {
	code      GoDBErrorCode
	errString string
}

func (e GoDBError) Error() string {
	return fmt.Sprintf("err: %s; msg: %s", e.code.String(), e.errString)
}

const (
	PageSize     int = 4096
	StringLength int = 32
)

type Page interface {
	// these methods are used by buffer pool to manage pages
	isDirty() bool
	setDirty(tid TransactionID, dirty bool)
	getFile() DBFile
}

type DBFile interface {
	insertTuple(t *Tuple, tid TransactionID) error
	deleteTuple(t *Tuple, tid TransactionID) error

	// methods used by buffer pool to manage retrieval of pages
	readPage(pageNo int) (Page, error)
	flushPage(page Page) error
	pageKey(pgNo int) any //uint64

	NumPages() int

	Operator
}

type Operator interface {
	Descriptor() *TupleDesc
	Iterator(tid TransactionID) (func() (*Tuple, error), error)
}

type BoolOp int

const (
	OpGt   BoolOp = iota
	OpLt   BoolOp = iota
	OpGe   BoolOp = iota
	OpLe   BoolOp = iota
	OpEq   BoolOp = iota
	OpNeq  BoolOp = iota
	OpLike BoolOp = iota
)

var BoolOpMap = map[string]BoolOp{
	">":    OpGt,
	"<":    OpLt,
	"<=":   OpLe,
	">=":   OpGe,
	"=":    OpEq,
	"<>":   OpNeq,
	"!=":   OpNeq,
	"like": OpLike,
}

func (i1 IntField) EvalPred(v2 DBValue, op BoolOp) bool {
	i2, ok := v2.(IntField)
	if !ok {
		return false
	}
	x1 := i1.Value
	x2 := i2.Value
	switch op {
	case OpEq:
		return x1 == x2
	case OpNeq:
		return x1 != x2
	case OpGt:
		return x1 > x2
	case OpGe:
		return x1 >= x2
	case OpLt:
		return x1 < x2
	case OpLe:
		return x1 <= x2
	default:
		return false
	}
}

func (i1 StringField) EvalPred(v2 DBValue, op BoolOp) bool {
	i2, ok := v2.(StringField)
	if !ok {
		return false
	}
	x1 := i1.Value
	x2 := i2.Value
	switch op {
	case OpEq:
		return x1 == x2
	case OpNeq:
		return x1 != x2
	case OpGt:
		return x1 > x2
	case OpGe:
		return x1 >= x2
	case OpLt:
		return x1 < x2
	case OpLe:
		return x1 <= x2
	case OpLike:
		s1, ok := any(i1).(string)
		if !ok {
			return false
		}
		regex, ok := any(i2).(string)
		if !ok {
			return false
		}
		regex = "^" + regex + "$"
		regex = strings.Replace(regex, "%", ".*?", -1)
		match, _ := regexp.MatchString(regex, s1)
		return match
	default:
		return false
	}
}
