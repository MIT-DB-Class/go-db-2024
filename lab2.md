# 6.5830/6.5831 Lab 2: GoDB Operators

**Assigned:** Tuesday September 24, 2024

**Due:** Monday October 21, 2024 by 11:59 PM ET

In this lab assignment, you will write a set of operators for GoDB to
implement table modifications (e.g., insert and delete records), filters,
joins, aggregates, etc. These will build on top of the foundation that you wrote
in Lab 1 to provide you with a database system that can perform simple queries
over multiple tables.

You do not need to implement transactions or locking in this lab.

The remainder of this document gives some suggestions about how to start coding,
describes a set of exercises to help you work through the lab, and discusses how
to hand in your code. This lab requires you to write a fair amount of code, so
we encourage you to **start early**!

------


<a name="starting"></a>

## 1. Getting started

You should begin with the code you submitted for Lab 1 (if you did not submit
code for Lab 1, or your solution didn't work properly, contact us to discuss
options). Additionally, we are providing extra source and test files for this
lab that are not in the original code distribution you received.

### 1.1. Getting Lab 2

You will need to add these new files to your release. The easiest way to do this
is to navigate to your project directory (probably called `go-db-hw-2024`)
and pull from the master GitHub repository:

```
$ cd go-db-hw-2024
$ git pull upstream main
```

### 1.2. Implementation hints

As before, we **strongly encourage** you to read through this entire document to
get a feel for the high-level design of GoDB before you write code.

We suggest exercises along this document to guide your implementation, but you
may find that a different order makes more sense for you. As before, we will
grade your assignment by looking at your code and verifying that you have passed
the unit and system tests. Note the code **only needs**
to pass the tests we indicate in this lab. 
See Section 3.4 for a complete discussion of grading and a list of the tests you will
need to pass.

Here's a rough outline of one way you might proceed with your GoDB
implementation; more details on the steps in this outline, including exercises,
are given in Section 2 below.

* Implement the operators `Filter (filter_op.go)` and `Join (join_op.go)` and verify that their
  corresponding tests work. The comments in these operators contain
  details about how they should work.

* Implement `CountAggState`, `SumAggState`, `AvgAggState`, `MinAggState`, and `MaxAggState` in `agg_state.go`. Here, you will write the
  logic that actually maintains an aggregation state over a particular field across
  multiple groups over a running sequence of input tuples. Use integer division for
  computing the average, since GoDB only supports integers. Only `CountAggState`, `MinAggState`, and `MaxAggState` needs to support both strings and integers, since the other operations do not
  make sense for strings. 

* Implement the `Aggregate (agg_op.go)` operator. As with other operators, aggregates
  implement the `Iterator` method so that they can be placed in GoDB
  query plans. Note that the output of an `Aggregate` operator is an aggregate
  value of an entire group for each call to the iterator, and that the aggregate
  constructor takes the aggregation and grouping fields.

* Implement the `Insert (insert_op.go)` and `Delete (delete_op.go)` operators. Like all operators,  `Insert`
  and `Delete` implement `Iterator`, accepting a stream of tuples to insert or
  delete and outputting a single tuple with an integer field that indicates the
  number of tuples inserted or deleted. 
  Check that the tests for inserting and deleting tuples work properly.
  
* Implement the `Project (project_op.go)`, `OrderBy (order_by_op.go)`, and `Limit (limit_op.go)` operators. 

Note that GoDB does not implement any kind of consistency or integrity
checking, so it is possible to insert duplicate records into a file, and there is
no way to enforce primary or foreign key constraints.

At this point, you should be able to run the SQL parser against the real database (e.g., the MBTA database from ps1), which is the goal of this lab.


## 2. GoDB Architecture and Implementation Guide

### 2.1. Filter and Join

Recall that GoDB OpIterator classes implement the operations of the
relational algebra. You will now implement two operators that will enable you to
perform queries that are slightly more interesting than a table scan.

* *Filter*: This operator only returns tuples that satisfy a predicate that is
  specified at construction.  Hence, it filters out any tuples that
  do not match the predicate.

* *Join*: This operator joins tuples from its two children according to an equality predicate that is specified at construction. Our current version of godb only supports joining with equality predicates `EqualityJoin`.
We only require
  a simple nested loop join, but you may explore more interesting join
  implementations. In particular, we will give a small amount of extra credit to those satisfying a stricter time-out requirement. Describe your implementation in your lab writeup.

For both of these operators, we have given you constructors so that you don't have to deal with the complexities of Go generics
and constructors for both integer and string fields.  You will need to implement the `Descriptor()` and `Iterator()` methods.

Note that both filters and joins take `Expr` objects that, in the case of joins or the left side of a filter, extract the field to be compared, or in the case of the right side of a filter, evaluate to a constant value.  We saw expressions in lab 1, but as a reminder, 
the idea here is that either side of a predicate can be an arbitrary arithmetic expression, e.g. a join expression can be:

`(t1.x + 7) = (t2.y * 4)`

To handle this, you will need to evaluate
the expression over the tuple and then use the  `getter` function to extract the value.
Here the getter takes a `DBValue` type and extracts either an `int64` or a `string`, depending
on the type of the filter or join (this way, you don't need to have different Iterator() implementations
for different types.)
For example, for the right field of the `joinOp` of type in the EqualityJoin's `Iterator()` implementation, you can get
the value for the right side of the join using:

```
v, _ := joinOp.rightField.EvalExpr(curT)
rightFieldVal := joinOp.getter(v)
```

**Exercise 1.**

Implement the skeleton methods in:

------

* godb/filter_op.go
* godb/join_op.go

------

Note that the implementation of `Iterator()`, particularly for join, is a bit tricky because your iterator will have to store the current progress of the iterator after returning each tuple.  You had to deal with this a bit in your heap file iterator in lab 1, but it is more complicated here.  Your implementation should not pre-compute the entire join or filter result;  instead, it should resume iterating through the child operator(s) at the point it left off after returning the previous tuple.

Recall that the result of a call to an `Iterator()` is a function that does the iteration and that this function can "capture" variables that are defined in the outer portion of the iterator.  To understand this, it may be helpful to look at the discussion of iterators and closures in lab 1, or review code such as [this example](https://go.dev/tour/moretypes/25).  Note that in this example, the `adder()`
function returns a function that captures a unique value of `sum` for each invocation of `adder()` -- so the to `adder` objects in `main()` will operate on different `sum` objects.  Your `Iterator()` implementation will want to capture the state of the iterator (how far it has iterated through the child iterators) outside of the function you return in a similar way.

At this point, your code should pass the unit tests in `filter_op_test.go` and the test `TestJoin` in `join_op_test.go`. You do not need to pass the test `TestBigJoinOptional` (this test will timeout and fail internally after 10 seconds).


**Exercise 1: Extra Credit**

Modify your join implementation in `join_op.go` to pass the test `TestBigJoinOptional`, which requires computing a large join in less than 10 seconds.  To get full credit for this test, your implementation needs to respect the `maxBufferSize` parameter passed to the join constructor.  This parameter is designed to limit the memory usage of your join implementation. You should not allocate an internal state in your join of any data structure that uses more than `maxBufferSize` tuples.  Describe in your writeup how you constrain memory consumption.  
Note that implementing the nested loop join as we described in class may not pass this test.
We will offer up to 10% extra credit for passing this test and your writeup.


### 2.2. Aggregates

The aggregate operator implements basic SQL aggregates with a `GROUP BY` clause. You will need to implement the five SQL aggregates (`COUNT`, `SUM`, `AVG`,
`MIN`, `MAX`) and support grouping over zero or more fields.

In order to calculate aggregates, we use an `AggState` interface, which merges
a new tuple into the existing calculation of an aggregate. The `AggState` is
told during construction what operation it should use for aggregation.
Subsequently, the client code should call `AggState.addTuple()` for
every tuple in the child iterator. After all tuples have been merged, the client
can retrieve an iterator of aggregation results. Each tuple in the result is a
pair of the form `(groupValue, aggregateValue)` unless the value of the group
by field was `Aggregator.groupByFields = nil`, in which case the result is a single
tuple of the form `(aggregateValue)`.

Note that this implementation requires space linear in the number of distinct
groups. For the purposes of this lab, you do not need to worry about the
situation where the number of groups exceeds available memory.

Similar to Exercise 1, we have provided the construction methods and fields for the `Aggregator` operator so that you only have to worry about the `Descriptor()` and `Iterator()` methods. 

Notice that in the fields of the `Aggregator` operator, `groupByFields` is an array of `Expr` objects. This is to support grouping by more than one `Expr`. Analogously, `newAggState` being an array of `AggState` is to support multiple aggregations per group at the same time (think `SELECT MAX(salary), AVG(salary), MIN(salary) FROM employees GROUP BY office_location;`).

As for `AggState`, the purpose is to maintain some running value for the aggregation operation (one of `COUNT`, `SUM`, `AVG`,
`MIN`, or `MAX`) when you go through the child iterators. For example, for the `SUM` operator, you will probably want to maintain some number representing the running sum up to the current tuple. Every aggregation operation needs to implement the interface methods: `Init`, `Copy`, `AddTuple`, `Finalize`, and `GetTupleDesc`. In general, we `Init`-ialize the aggregation state at the beginning, `AddTuple` of all relevant child tuples, and then call `Finalize` at the end to retrieve the aggregation results. This intuition should hint at how to implement the five aggregation operations and which fields to maintain. Furthermore, we have provided our implementation of the `COUNT` aggregation state, which may help you understand how some methods work. You will need to complete the implementation of other aggregation states.


**Exercise 2.**

Implement the skeleton methods in:

------

* godb/agg_state.go
* godb/agg_op.go

------

Again, for implementing the `Iterator()` method, you will want to make use of the "capture" functionality to store internal states such as how many result tuples have been iterated through. The logic of one possible implementation, of which we have provided a skeleton code, is as follows: on the first iterator call, firstly, we iterate through all the child tuples to collect aggregation results of all groups. Then, we create a `finalizedIter` iterator for iterating through the results of each group. Subsequent calls to the function will then simply be all redirected to `finalizedIter`. Our implementation uses three helper functions which you will have to implement: `extractGroupByKeyTuple` (given a tuple `t` from a child, return a tuple that identifies `t`'s group;  this tuple may contain multiple fields, one per group by attribute), `addTupleToGrpAggState` (given a tuple `t` from child and a pointer to an array of AggState `grpAggState`, add `t` into all aggregation states in the array), and `getFinalizedTuplesIterator` (given that all child tuples have been added, create an iterator that iterates through the finalized aggregate result of each group). We also handled the no group-by case for you, so you can assume there's always grouping when these helper functions are called. If you prefer, you may implement `Iterator()` in some other way that doesn't use our overall skeleton; we don't test the three helper methods, only the overall `Iterator()` method.

At this point, your code should pass the unit tests in `agg_op_test.go`.

### 2.3. Insertion and deletion

Now that you have written all of the aggregations, you will implement the `InsertOp` and `DeleteOp` operators.

For plans that implement `insert` and `delete` queries, the top most operator is
a special `InsertOp` or `DeleteOp` operator that modifies the pages of a specific `DBFile`. These operators
return the number of affected tuples. This is implemented by returning a single
tuple with one integer field, containing the count.

* *Insert*: This operator adds the tuples it reads from its child operator to
  the `insertFile` specified in its constructor. It should use the
  `insertFile.insertTuple()` method to do this.

* *Delete*: This operator deletes the tuples it reads from its child operator
  from the `deleteFile` specified in its constructor. It should use the
  `deleteFile.deleteTuple()` method to do this.

  Both of these operators should perform all of the inserts or deletes on the first invocation of the iterator, and then return the number of records inserted or deleted.  The returned tuple should have a single field "count" of type integer.
  You will need to implement the constructors, `Descriptor()` and  `Iterator()` for `InsertOp` and `DeleteOp` operators.
  The `Descriptor()` method should also return a descriptor with a single "count" field.

**Exercise 3.**

Implement the skeleton methods in:

------

* godb/insert_op.go
* godb/delete_op.go

------

Note that the correctness of this exercise heavily depends on the correctness of your code for lab1, especially, `heapFile.insertTuple()` and `heapFile.deleteTuple()`.
At this point, your code should pass the unit tests in `insert_op_test.go` and `delete_op_test.go`.


### 2.4. Projection


You will now implement the projection operation. Project iterates through its child, selects some of each tuple's fields, and returns them. Optionally, you will need to support the `DISTINCT` keyword, meaning that identical tuples should be returned only once. For example, given a dataset like:

```
sam, 25, $100,000
tim, 30, $75,000
mike, 35, $50,000
sam, 50, $150,000
```

If the query is:
```
SELECT name FROM table
```

The result should be:
```
sam
tim
mike
sam
```

But the following query:
```
SELECT DISTINCT name FROM table
```

Should instead produce:
```
sam
tim
mike
```


The list of fields to select, their names to be outputted by, whether the operation is `DISTINCT`, and the child operator is provided to the `NewProjectOp` constructor:
```
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) 
```
Here, `selectFields` is a list of expressions that can be extracted from the child operator's tuples (as in previous operators), and `outputNames` records the names that will populate the `Fname` fields in the tuple descriptor of the projection operation.

**Exercise 4.**

Implement the skeleton methods in:

------

* godb/project_op.go

------

At this point, your code should pass the unit tests in `project_op_test.go`. Passing `TestProjectDistinctOptional` is optional;  if you pass it, we will offer 5% additional extra credit on the lab.  Please be sure to describe how you implemented support for distinct in your writeup.


### 2.5. Order By


You will now implement the "order by" operation. It iterates through its child in a particular order. It needs to support ordering by more than one field, with each field in either ascending or descending order.   For example, consider the query:

```
SELECT name, age, salary
FROM table
ORDER BY name ASC, age DESC
```

Given a dataset like:
```
sam, 25, $100,000
tim, 30, $75,000
mike, 35, $50,000
sam, 50, $150,000
```

The above query should produce the result:
```
mike, 35, $50,000
sam, 50, $150,000
sam, 25, $100,000
tim, 30, $75,000
```

The list of fields to order by and the ascending/descending for each field provided to the `NewOrderBy` constructor:
```
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
```
Here, `orderByFields` is a list of expressions that can be extracted from the child operator's tuples (as in previous operators), and the ascending bitmap indicates whether the *i*th field in the `orderByFields` list should be ascending (true) or descending(false).

**Exercise 5.**

Implement the skeleton methods in:

------

* godb/order_by_op.go

------

At this point, your code should pass the unit tests in `order_by_test.go`.

### 2.6. Limit

You will now implement the limit operation. Limit iterates through its child and selects the first `n` tuples it sees. If the child returns `m < n` tuples, the limit operator only returns `m` tuples.

**Exercise 6.**

Implement the skeleton methods in:

------

* godb/limit_op.go

------

At this point, your code should pass the unit tests in `limit_op_test.go`. 

<a name="query_walkthrough"></a>

### 2.7. Composing Operators

At this point, you've implemented all the operators you need for a basic query plan.  You can construct a variety of queries by composing these operators together into plans.  We've provided a simple example in `simple_query_test.go`.  It runs code like the following :
```
func TestSimpleQuery(t *testing.T) {
	bp, c, err := MakeParserTestDatabase(10000)
	if err != nil {
		t.Fatalf("failed to create test database, %s", err.Error())
	}

	hf1, err := c.GetTable("t")
	if err != nil {
		t.Fatalf("no table t, %s", err.Error())
	}
	hf2, err := c.GetTable("t2")
	if err != nil {
		t.Fatalf("no table t2, %s", err.Error())
	}

	f_name := FieldExpr{FieldType{"name", "", StringType}}
	joinOp, err := NewJoin(hf1, &f_name, hf2, &f_name, 1000)
	if err != nil {
		t.Fatalf("failed to construct join, %s", err.Error())
	}
	f_age := FieldExpr{FieldType{"age", "t", IntType}}
	e_const := ConstExpr{IntField{30}, IntType}
	filterOp, err := NewFilter(&e_const, OpGt, &f_age, joinOp)
	if err != nil {
		t.Fatalf("failed to construct filter, %s", err.Error())
	}
	if filterOp == nil {
		t.Fatalf("filter op was nil")
	}
	if filterOp.Descriptor() == nil {
		t.Fatalf("filter op descriptor was nil")
	}
	sa := CountAggState{}
	expr := FieldExpr{filterOp.Descriptor().Fields[0]}
	sa.Init("count", &expr)
	agg := NewAggregator([]AggState{&sa}, filterOp)
	tid := NewTID()
	bp.BeginTransaction(tid)
	f, err := agg.Iterator(tid)
	if err != nil {
		t.Fatalf("failed to get iterator, %s", err.Error())
	}
	tup, err := f()
	if err != nil {
		t.Fatalf("failed to get tuple, %s", err.Error())
	}
	cnt2 := tup.Fields[0].(IntField).Value
	if cnt2 != 10 {
		t.Fatalf("expected 10 results, got %d", cnt2)
	}
}
```

This is equivalent to the query `SELECT COUNT(*) FROM t, t2 WHERE t.name = t2.name AND t.age > 30`.

You shouldn't need to implement anything extra to pass the `TestSimpleQuery` test.

### 2.8. Query Parser

Because it's very cumbersome to compose operators to make queries like this, we've provided a parser for you.  This allows you to input SQL queries and get a result set.  We've also built a query shell that allows you to interact with the parser.  To run it, type `go run main.go` from the top-level godb directory in your terminal (if an error message pops up, yu may need to run some `go get` as the error message suggested).
This will display:

```-bash ~/godb % go run main.go
Welcome to

    ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
    ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
    ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
    ▓▓▓▓▓▓▓▓░░░░░▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓░░░░░░░░▓▓▓▓▓▓▓░░░░░░░▓▓▓▓▓▓▓▓▓
    ▓▓▓▓▓░░░░░░░░▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓░░░░░░░░░░░▓▓▓▓░░░░░░░░░▓▓▓▓▓▓▓
    ▓▓▓▓░░░░░▓▓▓▓▓▓▓▓▓▓░░░░░ ▓▓▓▓░░░░▓▓▓░░░░░▓▓▓░░░░▓░░░░▓▓▓▓▓▓▓
    ▓▓▓▓░░░░▓▓▓░░░░▓▓░░░░░░░░░▓▓▓░░░░▓▓▓▓░░░░░▓▓░░░░░░░░▓▓▓▓▓▓▓▓
    ▓▓▓▓░░░░▓▓▓░░░░░░░░░▓▓▓░░░░▓▓░░░░▓▓▓▓░░░░░▓▓░░░░░░░░░▓▓▓▓▓▓▓
    ▓▓▓▓▓░░░░░░░░░░░▓░░░░░░░░░░▓▓░░░░░░░░░░░░▓▓▓░░░░░░░░░░▓▓▓▓▓▓
    ▓▓▓▓▓▓░░░░░░░░░▓▓▓░░░░░░░░▓▓▓░░░░░░░░░░▓▓▓▓▓░░░░░░░░░▓▓▓▓▓▓▓
    ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓

Type \h for help

>
```

Typing `\h` will give a list of commands you can input; for example, `\d` lists the tables and their schemas.  Tables are, by default, loaded from the file `catalog.txt`, but you can point to another catalog file.  Note that each table in the catalog is stored in a file called `<tablename>.dat`, where tablename is the name of the table. From this terminal, you can run `DROP`, `CREATE`, `INSERT`, `BEGIN`, `COMMIT/ROLLBACK`, and `SELECT` statements.  You can also load a CSV file into a table using the `\l` command.

The parser supports most of SQL with some limitations, including:

* No CTEs, window functions, recursive queries, or other SQL99 or later features (arbitrarily nested subqueries are fully supported)
* No OUTER joins (all joins are INNER)
* No USING clause for join predicates (you should write this to ON)
* No correlated subqueries
* No UPDATEs


When you first run the console, it will load a small test catalog containing two identical tables of people and ages.  You can see the schemas of these tables using the `\d` command.  If you have fully implemented the operators from the previous exercises (including DISTINCT) you should be able to pass this test.  Because these test queries use DISTINCT, we will not grade you on these particular queries but may have hidden test cases that run a few SQL queries against your lab, so you should be sure to confirm that at least simple queries run.  

As an example, we have loaded the ps1 mbta dataset into GoDB format.  You can download it from [here](https://www.dropbox.com/scl/fi/l27l17fg6mo3d4jjihmls/transitdb.zip?rlkey=890c1omvwevm6n4us10d7m11j).  Note that all columns are either strings or ints;  floats have been cast to ints in this database.
Download the `transitdb` folder (you may be asked to login or create Dropdox account) and put it in your top-level godb directory.
Afterward, you can connect to over the console using the `\c`
command:
```
> \c transitdb/transitdb.catalog
Loaded transitdb/transitdb.catalog
gated_station_entries (service_date string, time string, station_id string, line_id string, gated_entries int)
lines (line_id string, line_name string)
routes (route_id int, line_id string, first_station_id string, last_station_id string, direction int, direction_desc string, route_name string)
stations (station_id string, station_name string)
rail_ridership (season string, line_id string, direction int, time_period_id string, station_id string, total_ons int, total_offs int, number_service_days int, average_ons int, average_offs int, average_flow int)
station_orders (route_id int, station_id string, stop_order int, distance_from_last_station_miles int)
time_periods (time_period_id string, day_type string, time_period string, period_start_time string, period_end_time string)
```

Once it is loaded, you should be able to run a query. For example, to find the first and last station of each line, you can write:
``` 
> SELECT line_name,
>        direction_desc,
>        s1.station_name AS first_station,
>        s2.station_name AS last_station
> FROM routes
> JOIN lines ON lines.line_id = routes.line_id
> JOIN stations s1 ON first_station_id = s1.station_id
> JOIN stations s2 ON last_station_id = s2.station_id
> ORDER BY line_name ASC, direction_desc ASC, first_station ASC, last_station ASC;
          line_name          |        direction_desc       |        first_station        |         last_station        |
         "Blue Line"         |             East            |           Bowdoin           |          Wonderland         |
         "Blue Line"         |             West            |          Wonderland         |           Bowdoin           |
         "Green Line"        |             East            |       "Boston College"      |     "Government Center"     |
         "Green Line"        |             East            |      "Cleveland Circle"     |     "Government Center"     |
         "Green Line"        |             East            |        "Heath Street"       |           Lechmere          |
         "Green Line"        |             East            |          Riverside          |       "North Station"       |
         "Green Line"        |             West            |     "Government Center"     |       "Boston College"      |
         "Green Line"        |             West            |     "Government Center"     |      "Cleveland Circle"     |
         "Green Line"        |             West            |       "North Station"       |          Riverside          |
         "Green Line"        |             West            |           Lechmere          |        "Heath Street"       |
      "Mattapan Trolley"     |           Inbound           |           Mattapan          |           Ashmont           |
      "Mattapan Trolley"     |           Outbound          |           Ashmont           |           Mattapan          |
        "Orange Line"        |            North            |        "Forest Hills"       |         "Oak Grove"         |
        "Orange Line"        |            South            |         "Oak Grove"         |        "Forest Hills"       |
          "Red Line"         |            North            |           Ashmont           |           Alewife           |
          "Red Line"         |            North            |          Braintree          |           Alewife           |
          "Red Line"         |            South            |           Alewife           |           Ashmont           |
          "Red Line"         |            South            |           Alewife           |          Braintree          |
(18 results)
57.01075ms
```

You can also view the query plan generated for the query by appending the "EXPLAIN" keyword to a query, e.g.:
```
> explain SELECT line_name,
>        direction_desc,
>        s1.station_name AS first_station,
>        s2.station_name AS last_station
> FROM routes
> JOIN lines ON lines.line_id = routes.line_id
> JOIN stations s1 ON first_station_id = s1.station_id
> JOIN stations s2 ON last_station_id = s2.station_id
> ORDER BY line_name ASC, direction_desc ASC, first_station ASC, last_station ASC;

Order By line_name,direction_desc,first_station,last_station,
    Project lines.line_name,routes.direction_desc,s1.station_name,s2.station_name, -> [line_name direction_desc first_station last_station]
        Join, routes.last_station_id == s2.station_id
            Join, routes.first_station_id == s1.station_id
                Join, lines.line_id == routes.line_id
                    Heap Scan transitdb/lines.dat
                    Heap Scan transitdb/routes.dat
                Heap Scan transitdb/stations.dat
            Heap Scan transitdb/stations.dat
```

**Exercise 7.**

Run a few queries against the transitdb to make sure your operator implementations are working.  

You should also be able to pass `TestParseEasy` in `easy_parser_test.go`.  This test runs a few SQL queries against the `catalog.txt` catalog that we have provided.  Note that it works by comparing your results to a set of saved CSV files in the `savedresults` directory.

You have now completed this lab. Good work!



## 3. Logistics

You must submit your code (see below) as well as a short (2 pages, maximum)
writeup describing your approach. This writeup should:

* Describe any design decisions you made, including your choice of join and aggregate operator implementation. If you used something other than a nested-loops join, describe the
  tradeoffs of the algorithm you chose.  If you implemented support for distinct in `project_op.go`, describe how you implemented it.


* Discuss and justify any changes you made to the API.

* Describe any missing or incomplete elements of your code.

* Describe how long you spent on the lab, and whether there was anything you
  found particularly difficult or confusing.

### 3.1. Collaboration

This lab should be manageable for a single person, but if you prefer to work
with a partner, this is also OK. Larger groups are not allowed. Please indicate
clearly who you worked with, if anyone, on your individual writeup.

### 3.2. Submitting your assignment

We will be using Gradescope to autograde all programming assignments. You should
have all been invited to the class instance; if not, please check Piazza for an
invite code. If you are still having trouble, let us know, and we can help you
set up. You may submit your code multiple times before the deadline; we will use
the latest version as determined by Gradescope. Place the write-up in a file
called `lab2-writeup.txt` with your submission.

If you are working with a partner, only one person needs to submit to
Gradescope. However, make sure to add the other person to your group. Also, note
that each member must have their own writeup. Please add your Kerberos username
to the file name and in the writeup itself (e.g., `lab2-writeup-username1.txt`
and `lab2-writeup-username2.txt`).

The easiest way to submit to Gradescope is with `.zip` files containing your
code. On Linux/macOS, you can do so by running the following command:

```bash
$ zip -r submission.zip godb/ lab2-writeup.txt

# If you are working with a partner:
$ zip -r submission.zip godb/ lab2-writeup-username1.txt lab2-writeup-username2.txt
```

### 3.3. Submitting a bug

Please submit (friendly!) bug reports to
[6.5830-staff@mit.edu](mailto:6.5830-staff@mit.edu) or as a post on [Piazza](https://piazza.com/class/m06qshnnxi85if). When you do, please try to
include:

* A description of the bug.
* A `.go` file with test functions that we can drop into the `godb` directory, compile, and run.
* A `.txt` file with the data that reproduces the bug.

If you are the first person to report a particular bug in the code, we will give
you a candy bar or equivalence!

<a name="grading"></a>

### 3.4 Grading


75% of your grade will be based on whether or not your code passes the system
test suite we will run over it. These tests will be a superset of the tests we
have provided. Before handing in your code, you should make sure it produces no
errors (passes all of the tests) when you run `go test` in the `godb` directory.

**Important:** Before testing, Gradescope will replace the go test files with our version of these files.
This means you should make sure that your code passes the unmodified tests.

You should get immediate feedback (up to a few minutes) and error outputs for failed visible tests (if any)
from Gradescope after submission. There may exist several hidden tests (a small percentage) that will be on Gradescope but not released to you.
The score given will be your grade for the
auto-graded portion of the assignment. An additional 25% of your grade will be
based on the quality of your writeup and our subjective evaluation of your code.
This part will also be published on Gradescope after we finish grading your
assignment.

We had a lot of fun designing this assignment, and we hope you enjoy hacking on
it!
