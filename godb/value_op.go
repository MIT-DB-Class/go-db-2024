package godb

// Methods to expose an array of constant expressions as tuples to iterate
// through (e.g., for insert statements or select from a constant list).

type ValueOp struct {
	td    *TupleDesc
	exprs []([]Expr)
}

func NewValueOp(exprs []([]Expr)) *ValueOp {
	var td TupleDesc
	if len(exprs) > 0 {
		first := exprs[0]
		fts := make([]FieldType, len(first))
		for i, field := range first {
			fts[i] = field.GetExprType()
		}
		td = TupleDesc{fts}
	}

	return &ValueOp{&td, exprs}
}

func (v *ValueOp) Descriptor() *TupleDesc {
	return v.td
}

func (v *ValueOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	curTup := 0
	return func() (*Tuple, error) {
		if curTup >= len(v.exprs) {
			return nil, nil
		}
		tup := v.exprs[curTup]
		fields := make([]DBValue, len(tup))
		for i, field := range tup {
			outf, err := field.EvalExpr(nil)
			if err != nil {
				return nil, err
			}
			fields[i] = outf
		}

		curTup++

		return &Tuple{*v.td, fields, nil}, nil
	}, nil
}
