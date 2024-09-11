package godb

import (
	"fmt"
	"os"
	"testing"
)

func TestLab1Query(t *testing.T) {
	if os.Getenv("LAB") == "5" {
		t.Skip("This test is only valid up to Lab 4. Skipping")
	}
	bp, _, err := MakeTestDatabase(10, "catalog.txt")
	if err != nil {
		t.Fatalf("Failed to initialize test database")
	}
	f1 := FieldType{"name", "", StringType}
	f2 := FieldType{"age", "", IntType}
	td := TupleDesc{[]FieldType{f1, f2}}
	sum, err := computeFieldSum(bp, "lab1_test.csv", td, "age")
	if err != nil {
		fmt.Println(err)
	}
	if sum != 1111 {
		t.Fatalf("expected sum of 1111, got %d", sum)
	}
}
