package godb

import (
	"fmt"
	"math/rand"
	"time"
)

//Expressions can be applied to tuples to get concrete values.  They
//encapsulate constates, simple fields, and functions over multiple other
//expressions.  We have provided the expression methods for you;  you will need
//use the [EvalExpr] method  in your operator implementations to get fields and
//other values from tuples.

type Expr interface {
	EvalExpr(t *Tuple) (DBValue, error) //DBValue is either IntField or StringField
	GetExprType() FieldType             //Return the type of the Expression
}

type FieldExpr struct {
	selectField FieldType
}

func (f *FieldExpr) EvalExpr(t *Tuple) (DBValue, error) {
	outTup, err := t.project([]FieldType{f.selectField})
	if err != nil {
		//fmt.Printf("err in project: %s", err.Error())
		return nil, err
	}
	return outTup.Fields[0], nil

}

func (f *FieldExpr) GetExprType() FieldType {
	return f.selectField
}

type ConstExpr struct {
	val       DBValue
	constType DBType
}

func (c *ConstExpr) GetExprType() FieldType {
	return FieldType{"const", fmt.Sprintf("%v", c.val), c.constType}
}

func (c *ConstExpr) EvalExpr(_ *Tuple) (DBValue, error) {
	return c.val, nil
}

type FuncExpr struct {
	op   string
	args []*Expr
}

func (f *FuncExpr) GetExprType() FieldType {
	fType, exists := funcs[f.op]
	//todo return err
	if !exists {
		return FieldType{f.op, "", IntType}
	}
	ft := FieldType{f.op, "", IntType}
	for _, fe := range f.args {
		fieldExpr, ok := (*fe).(*FieldExpr)
		if ok {
			ft = fieldExpr.GetExprType()
		}
	}
	return FieldType{ft.Fname, ft.TableQualifier, fType.outType}

}

type FuncType struct {
	argTypes []DBType
	outType  DBType
	f        func([]any) any
}

var funcs = map[string]FuncType{
	//note should all be lower case
	"+":                     {[]DBType{IntType, IntType}, IntType, addFunc},
	"-":                     {[]DBType{IntType, IntType}, IntType, minusFunc},
	"*":                     {[]DBType{IntType, IntType}, IntType, timesFunc},
	"/":                     {[]DBType{IntType, IntType}, IntType, divFunc},
	"mod":                   {[]DBType{IntType, IntType}, IntType, modFunc},
	"rand":                  {[]DBType{}, IntType, randIntFunc},
	"sq":                    {[]DBType{IntType}, IntType, sqFunc},
	"getsubstr":             {[]DBType{StringType, IntType, IntType}, StringType, subStrFunc},
	"epoch":                 {[]DBType{}, IntType, epoch},
	"datetimestringtoepoch": {[]DBType{StringType}, IntType, dateTimeToEpoch},
	"datestringtoepoch":     {[]DBType{StringType}, IntType, dateToEpoch},
	"epochtodatetimestring": {[]DBType{IntType}, StringType, dateString},
	"imin":                  {[]DBType{IntType, IntType}, IntType, minFunc},
	"imax":                  {[]DBType{IntType, IntType}, IntType, maxFunc},
}

func ListOfFunctions() string {
	fList := ""
	for name, f := range funcs {
		args := "("
		argList := f.argTypes
		hasArg := false
		for _, a := range argList {
			if hasArg {
				args = args + ","
			}
			switch a {
			case IntType:
				args = args + "int"
			case StringType:
				args = args + "string"
			}
			hasArg = true
		}
		args = args + ")"
		fList = fList + "\t" + name + args + "\n"
	}
	return fList
}
func minFunc(args []any) any {
	first := args[0].(int64)
	second := args[1].(int64)
	if first < second {
		return first
	}
	return second
}

func maxFunc(args []any) any {
	first := args[0].(int64)
	second := args[1].(int64)
	if first >= second {
		return first
	}
	return second
}

func dateTimeToEpoch(args []any) any {
	inString := args[0].(string)
	tt, err := time.Parse(time.UnixDate, inString)
	if err != nil {
		return int64(0)
	}
	return int64(time.Time.Unix(tt))
}

func dateToEpoch(args []any) any {
	inString := args[0].(string)
	tt, err := time.Parse("2006-01-02", inString)
	if err != nil {
		return int64(0)
	}
	return int64(time.Time.Unix(tt))
}

func dateString(args []any) any {
	unixTime := args[0].(int64)
	t := time.Unix(unixTime, 0)
	strDate := t.Format(time.UnixDate)
	return strDate
}

func epoch(args []any) any {
	t := time.Now()
	return time.Time.Unix(t)
}

func randIntFunc(args []any) any {
	return int64(rand.Int())
}

func modFunc(args []any) any {
	return args[0].(int64) % args[1].(int64)
}

func divFunc(args []any) any {
	return args[0].(int64) / args[1].(int64)
}

func timesFunc(args []any) any {
	return args[0].(int64) * args[1].(int64)
}

func minusFunc(args []any) any {
	return args[0].(int64) - args[1].(int64)
}

func addFunc(args []any) any {
	return args[0].(int64) + args[1].(int64)
}

func sqFunc(args []any) any {
	return args[0].(int64) * args[0].(int64)
}

func subStrFunc(args []any) any {
	stringVal := args[0].(string)
	start := args[1].(int64)
	numChars := args[2].(int64)

	var substr string
	if start < 0 || start > int64(len(stringVal)) {
		substr = ""
	} else if start+numChars > int64(len(stringVal)) {
		substr = stringVal[start:]
	} else {
		substr = stringVal[start : start+numChars]
	}

	return substr
}

func (f *FuncExpr) EvalExpr(t *Tuple) (DBValue, error) {
	fType, exists := funcs[f.op]
	if !exists {
		return nil, GoDBError{ParseError, fmt.Sprintf("unknown function %s", f.op)}
	}
	if len(f.args) != len(fType.argTypes) {
		return nil, GoDBError{ParseError, fmt.Sprintf("function %s expected %d args", f.op, len(fType.argTypes))}
	}
	argvals := make([]any, len(fType.argTypes))
	for i, argType := range fType.argTypes {
		arg := *f.args[i]
		if arg.GetExprType().Ftype != argType {
			typeName := "string"
			switch argType {
			case IntType:
				typeName = "int"
			}
			return nil, GoDBError{ParseError, fmt.Sprintf("function %s expected arg of type %s", f.op, typeName)}
		}
		val, err := arg.EvalExpr(t)
		if err != nil {
			return nil, err
		}
		switch argType {
		case IntType:
			argvals[i] = val.(IntField).Value
		case StringType:
			argvals[i] = val.(StringField).Value
		}
	}
	result := fType.f(argvals)
	switch fType.outType {
	case IntType:
		return IntField{result.(int64)}, nil
	case StringType:
		return StringField{result.(string)}, nil
	}
	return nil, GoDBError{ParseError, "unknown result type in function"}
}
