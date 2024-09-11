package godb

import (
	"fmt"
	"testing"
)

func TestLab1Query(t *testing.T) {
	f1 := FieldType{"name", "", StringType}
	f2 := FieldType{"age", "", IntType}
	td := TupleDesc{[]FieldType{f1, f2}}
	sum, err := computeFieldSum("lab1_test.csv", td, "age")
	if err != nil {
		fmt.Println(err)
	}
	if sum != 1111 {
		t.Fatalf("expected sum of 1111, got %d", sum)
	}
}
