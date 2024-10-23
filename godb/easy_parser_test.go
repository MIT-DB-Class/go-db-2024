package godb

import (
	"fmt"
	"os"
	"testing"
)

type Query struct {
	SQL     string
	Ordered bool
}

func TestParseEasy(t *testing.T) {
	queries := []Query{
		{SQL: "select sum(age) as s from t group by t.name having s > 30", Ordered: false},
		{SQL: "select sum(age + 10) , sum(age) from t", Ordered: false},
		{SQL: "select min(age) + max(age) from t", Ordered: false},
		{SQL: "select * from t order by t.age, t.name limit 1+2", Ordered: true},
		{SQL: "select t.name, t.age from t join t2 on t.name = t2.name, t2 as t3 where t.age < 50 and t3.age = t.age order by t.age asc, t.name asc", Ordered: true},
		{SQL: "select sq(sq(5)) from t", Ordered: false},
		{SQL: "select 1, name from t", Ordered: false},
		{SQL: "select age, name from t", Ordered: false},
		{SQL: "select t.name, sum(age) totage from t group by t.name", Ordered: false},
		{SQL: "select t.name, t.age from t join t2 on t.name = t2.name where t.age < 50", Ordered: false},
		{SQL: "select name from (select x.name from (select t.name from t) x)y order by name asc", Ordered: true},
		{SQL: "select age, count(*) from t group by age", Ordered: false},
	}
	save := false        //set save to true to save the output of the current test run as the correct answer
	printOutput := false //print the result set during testing

	bp, c, err := MakeParserTestDatabase(10)
	if err != nil {
		t.Fatalf("failed to create test database, %s", err.Error())
	}

	qNo := 0
	for _, query := range queries {
		tid := BeginTransactionForTest(t, bp)
		qNo++

		qType, plan, err := Parse(c, query.SQL)
		if err != nil {
			t.Fatalf("failed to parse, q=%s, %s", query.SQL, err.Error())
		}
		if plan == nil {
			t.Fatalf("plan was nil")
		}
		if qType != IteratorType {
			continue
		}

		var outfile *HeapFile
		var outfile_csv *os.File
		var resultSet []*Tuple
		fname := fmt.Sprintf("savedresults/q%d-easy-result.csv", qNo)

		if save {
			os.Remove(fname)
			outfile_csv, err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				t.Fatalf("failed to open CSV file (%s)", err.Error())
			}
			//outfile, _ = NewHeapFile(fname, plan.Descriptor(), bp)
		} else {
			fname_bin := fmt.Sprintf("savedresults/q%d-easy-result.dat", qNo)
			os.Remove(fname_bin)
			desc := plan.Descriptor()
			if desc == nil {
				t.Fatalf("descriptor was nil")
			}

			outfile, _ = NewHeapFile(fname_bin, desc, bp)
			if outfile == nil {
				t.Fatalf("heapfile was nil")
			}
			f, err := os.Open(fname)
			if err != nil {
				t.Fatalf("csv file with results was nil (%s)", err.Error())
			}
			err = outfile.LoadFromCSV(f, true, ",", false)
			if err != nil {
				t.Fatalf(err.Error())
			}

			resultIter, err := outfile.Iterator(tid)
			if err != nil {
				t.Fatalf(err.Error())
			}
			for {
				tup, err := resultIter()
				if err != nil {
					t.Fatalf(err.Error())
				}

				if tup != nil {
					resultSet = append(resultSet, tup)
				} else {
					break
				}
			}
		}

		if printOutput || save {
			fmt.Printf("Doing %s\n", query.SQL)
			iter, err := plan.Iterator(tid)
			if err != nil {
				t.Fatalf("%s", err.Error())

			}
			nresults := 0
			if save {
				fmt.Fprintf(outfile_csv, "%s\n", plan.Descriptor().HeaderString(false))
			}
			fmt.Printf("%s\n", plan.Descriptor().HeaderString(true))
			for {
				tup, err := iter()
				if err != nil {
					t.Errorf("%s", err.Error())
					break
				}
				if tup == nil {
					break
				} else {
					fmt.Printf("%s\n", tup.PrettyPrintString(true))
				}
				nresults++
				if save {
					fmt.Fprintf(outfile_csv, "%s\n", tup.PrettyPrintString(false))
					//outfile.insertTuple(tup, tid)
				}
			}
			fmt.Printf("(%d results)\n\n", nresults)
		}
		if save {
			bp.FlushAllPages()
			outfile.bufPool.CommitTransaction(tid)
			outfile_csv.Close()
		} else {
			iter, err := plan.Iterator(tid)
			if err != nil {
				t.Fatalf("%s", err.Error())
			}
			if query.Ordered {
				err = CheckIfOutputMatches(iter, resultSet)
			} else {
				err = CheckIfOutputMatchesUnordered(iter, resultSet)
			}
			if err != nil {
				t.Errorf("query '%s' did not match expected result set: %v", query.SQL, err)
				verbose := true
				if verbose {
					fmt.Print("Expected: \n")
					for _, r := range resultSet {
						fmt.Printf("%s\n", r.PrettyPrintString(true))
					}
				}
			}
		}
	}
}
