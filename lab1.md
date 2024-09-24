# 6.5830/6.5831 Lab 1: GoDB

**Assigned:** Wednesday September 11, 2024

**Due:** Wednesday September 25, 2024 by 11:59 PM ET

<!--
**Bug Update:** We have a [page](bugs.html) to keep track
of SimpleDB bugs that you or we find. Fixes for bugs/annoyances will also be
posted there. Some bugs may have already been found, so do take a look at the
page to get the latest version/ patches for the lab code.
-->

In the lab assignments in 6.5830/6.5831 you will write a basic database
management system called GoDB. For this lab, you will focus on implementing
the core modules required to access stored data on disk; in future labs, you
will add support for various query processing operators, as well as
transactions, locking, and concurrent queries.

Unlike in the old edition of the course, we started implementing the labs in Go starting from last year.
Since Course 6 has moved away from teaching Java in our software engineering classes, it
makes less sense to use Java for our systems classes as well.  Go is a simple, modern language
that is easy to learn and efficient.  It uses garbage collection so is far easier to program than
e.g., C or C++.  In addition, few students in the class will have extensive experience with Go,
so it "levels the playing field", unlike Java where some students know it very well
and others have little experience with it.

Because this is the second year we are using Go, there may still very well be bugs in the labs or things
that are not clear.  Please be patient with us;  we will do our best to be responsive and help you
resolve issues and ambiguity.  We have also reduced the number of required labs from 4 to 3 (adding a Go Tutorial Lab 0)
in place of one of the labs.

For GoDB We have provided you with a set of mostly
unimplemented methods, which you will need to fill in.
We will grade your code by running a set of tests written using
[Go testing](https://pkg.go.dev/testing), similar to Lab 0. We provide you with part of these tests which you can use
 for testing your code yourself. However, we will also reserve some hidden tests for evaluating your code.
We therefore encourage you to develop your own test suite in addition to the tests supplied.

The remainder of this document describes the basic architecture of GoDB,
gives some suggestions about how to start coding, and discusses how to hand in
your lab.

We **strongly recommend** that you start as early as possible on this lab. It
requires you to write a fair amount of code!


##  0.  Find bugs, be patient, earn treats

GoDB is a relatively complex piece of code, and we have written it from scratch fairly recently. It is very possible you are
going to find bugs, inconsistencies, and unclear
documentation, etc.

If something is suspicious or unclear, you are welcome to try to figure it out
yourself and make a public Piazza post, or send us a friendly email. We promise to be as attentive as possible ans help out by posting bug
fixes, new commits to the HW repo, etc., as bugs and issues are reported.

<p>...and if you find a bug in our code, we'll give you a yummy treat (see
[Section 3.3](#submitting-a-bug))!



## 1. Getting started

GoDB uses the [Go tool suite](https://pkg.go.dev/cmd/go) to compile the code
and run tests.  You will need to [install Go](https://go.dev/doc/install), which is available
for all major operating systems. For installation instructions, please refer to Lab 0.


### 1.1. Tests

In go, tests are defined as functions that begin with the name "Test".
To run all of the GoDB tests, cd into the godb directory and type `go test`.  This will run
all of the tests that we have defined;  following go conventions, we have placed all of the
GoDB tests in files named `class_test.go`, e.g., `heap_file_test.go`.


Running `go test` after setting up the work directory, you should see output similar to:

```
go test

--- FAIL: TestGetPage (0.00s)
    buffer_pool_test.go:27: failed to get page 0 (err = <nil>)
--- FAIL: TestCreateAndInsertHeapFile (0.00s)
    heap_file_test.go:60: HeapFile iterator expected 2 tuples, got 0
--- FAIL: TestDeleteHeapFile (0.00s)
    heap_file_test.go:73: HeapFile iterator expected 1 tuple
--- FAIL: TestSerializeSmallHeapFile (0.00s)
    heap_file_test.go:118: HeapFile iterator expected 4 tuples, got 0
--- FAIL: TestSerializeLargeHeapFile (0.00s)
    heap_file_test.go:118: HeapFile iterator expected 4000 tuples, got 0
--- FAIL: TestSerializeVeryLargeHeapFile (0.00s)
    heap_file_test.go:118: HeapFile iterator expected 40000 tuples, got 0
--- FAIL: TestLoadCSV (0.00s)
    heap_file_test.go:143: Load failed, code 4;  err: Descriptor was nil

```

The output above indicates that all of the tests failed; this is
because the code we have given you doesn't yet work. As you complete parts of
the lab, you will work towards passing additional unit tests, which are located in files named like `*_test.go` under `godb/`.


### 1.2. Working with an IDE

We strongly recommend using VSCode, which is a modern IDE with good support for
GoLang.  

#### 1.2.1 Setting Up VSCode

VSCode is a popular free extensible code editor that works with many languages,
including Go. You can find installation instructions
[here](https://code.visualstudio.com/docs/setup/setup-overview), though we expect you to have already installed VSCode and Go correctly via the Lab 0 installation instructions.

The following is specific to setting up the godb work directory for Lab 1.

1. `git clone` from the Lab1 Github repo. 
2. Open VSCode, then File -> Open Folder -> Choose the cloned folder 
3. Still in VSCode, Terminal -> New Terminal
4. Run the following in the terminal:
```
go get main
cd godb
go get ../godb
go test
```

You should then see the failed test messages as described in the previous section. You can refer to the main README of the repo or the pdf handout for additional git suggestions.



### 1.3. Implementation hints

Before beginning to write code, we **strongly encourage** you to read through
this entire document to get a feel for the high-level design of GoDB.

You will need to fill in any piece of code that is not implemented. You may need to add private
methods and/or helper classes. You may change APIs, but make sure our
 tests still run and make sure to mention, explain, and
justify your decisions in your writeup.

In addition to the methods that you need to fill out for this lab, the class
interfaces contain some methods that you need not implement until subsequent
labs. These will either be indicated per method, for example `AbortTransaction` is a method you do not need to implement for lab 1:

```golang
// Abort the transaction, releasing locks
// Because GoDB is a FORCE/NO STEAL, none of the pages
// tid has dirtired will be on disk so it is sufficient to just release
// locks to abort.
// You do not need to implement this method for lab 1
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
    // TODO: some code goes here
}
```


The code that you submit should compile and run without having to modify these methods.

We suggest exercises along this document to guide your implementation, but you
may find that a different order makes more sense for you.

**Here's a rough outline of one way you might proceed with your Lab 1
implementation:**

---

* We have provided you with a set of core types and interfaces in `types.go`.  Review these as you will need to use them.
* Implement the missing functions in `tuple.go`.  These methods allow you to compare tuples and tuple descriptors.
* Implement the `buffer_pool.go` constructor and the `GetPage()` method.  You can ignore the transaction methods and use the `TransactionID`'s in other signatures in the trivial way for lab 1.
* Implement the missing methods in `heap_file.go` and `heap_page.go`.  
These allow you to create heap files, insert and delete records from them,
and iterate through them.  Some of the methods have already been written for you.  
* At this point, you should be able to pass all the unit tests except for the `lab1_query_test.go` integration test.
* Implement `computeFieldSum` in `lab1_query.go` according to the doc string. This exercise allows you to write and run the first query with your version of GoDB. After finishing, you shoudl be able to pass `lab1_query_test.go` as well, which is the goal of the lab.

---

Section 2 below walks you through these implementation steps and the unit tests
corresponding to each one in more detail.

### 1.4. Transactions, locking, and recovery

As you look through the interfaces we have provided you, you will see a number
of references to transactions. You do not need to support
these features in this lab, but you should keep these parameters in the
interfaces of your code because you will be implementing transactions and
concurrency controll in a future lab. The test code we have provided you with generates a
 transaction ID that is passed into the operators of the query it runs; you
should pass this transaction ID into other operators and the buffer pool.

## 2. GoDB Architecture and Implementation Guide

GoDB consists of:

* Structures that represent fields, tuples, and tuple schemas;
* Methods that apply predicates and conditions to tuples;
* One or more access methods (e.g., heap files) that store relations on disk and
  provide a way to iterate through tuples of those relations;
* A collection of operator classes (e.g., select, join, insert, delete, etc.)
  that process tuples;
* A buffer pool that caches active tuples and pages in memory and handles
  concurrency control and transactions (neither of which you need to worry about
  for this lab); and,
* A catalog that stores information about available tables and their schemas.

GoDB does not include many things that you may think of as being a part of a
"database system." In particular, GoDB does not have:

* (In this lab), a SQL front end or parser that allows you to type queries
  directly into GoDB. Instead, queries are built up by chaining a set of
  operators together into a hand-built query plan (see [Section
  2.6](#query_walkthrough)). We will provide a simple parser for use in later
  labs.
* Views.
* Data types except integers and fixed length strings.
* (In this lab) Query optimizer.
* (In this lab) Indices.

In the rest of this Section, we describe each of the main components of GoDB
that you will need to implement in this lab. You should use the exercises in
this discussion to guide your implementation. This document is by no means a
complete specification for GoDB; you will need to make decisions about how
to design and implement various parts of the system. Note that for Lab 1 you do
not need to implement any operators (e.g., select, join, project) except
sequential scan as a part of the `heap_file.go` file.
You will add support for additional operators in future labs.

### 2.1. Core Classes

The main database state is stored in
the catalog (the list of all the tables in the database - you will not need to do anything for this in lab 1),
the buffer pool (the collection of database file pages that are currently resident in memory), and
the various data files (e.g., heap files) that store data on disk in pages.
You will implement the buffer pool and heap files in this lab.

### 2.2. Operators and Iterators

Queries in GoDB are implemented using the "iterator" model -- essentially each operator (select, project, join, scan, etc) implements the Operator interface

```golang
type Operator interface {
    Descriptor() *TupleDesc
    Iterator(tid TransactionID) (func() (*Tuple, error), error)
}
```

The Iterator method of each operator returns a function that iterates through its tuples.  Most operators take a "child" operator as a parameter to their constructor that they iterate through and apply their logic to.  Access methods like heap files that implement scans and index lookups don't have children:  they read data directly from files (or caches) and iterate through them.  The advantage of having operators all implement the iterator interface is that operators can be composed arbitrarily -- i.e., a join can read from a filter, or a filter can read from a project which can read from a join of two heap files, without needing to have specific implementations of each operator for each type of child operator.

If you haven't written code that returns functions like this before, it can be a bit tricky.  We use a pattern in GoDB based on ["closures"](https://go.dev/tour/moretypes/25). Here is an example where we iterate through odd numbers using a closure. `newOdd()` returns a function (a closure) that increments `n` and returns the incremented value. Note that every time you call `newOdd()` it instantiates a new variable `n` that can be used by the returned function.
```golang
func newOdd() func() int {
    n := 1
    // closure can reference and use the variable n
    return func() int {
        n += 2
        return n
    }
}

func main() {
    iter := newOdd()
    for {
        fmt.Printf("next odd is %d\n", iter())
    }
}
```

### 2.3. Fields and Tuples

The `Tuple` struct in GoDB is used to store the in-memory value of a database tuple.  
They consist of a collection of fields implementing the `DBValue`
interface.  Different
data types (e.g., `IntField`, `StringField`) implement `DBValue`.  `Tuple` objects are created by
the underlying access methods (e.g., heap files, or B-trees), as described in
the next section.  Tuples also have a type (or schema), called a _tuple
descriptor_, represented by a `TupleDesc` struct, which consists of a
collection of `FieldType` objects, one per field in the tuple, each of which
describes the type of the corresponding field.



### Exercise 1

**Implement the skeleton methods in:**

---
* tuple.go
---

At this point, your code should pass the unit tests in `tuple_test.go`.

### 2.4. BufferPool

The buffer pool (class `BufferPool` in GoDB) is responsible for caching
pages in memory that have been recently read from disk. All operators read and
write pages from various files on disk through the buffer pool. It consists of a
fixed number of pages, defined by the `numPages` parameter to the `BufferPool`
constructor `NewBufferPool`.  

For this lab,
you need to implement the constructor and the `BufferPool.getPage()` method
used by the `HeapFile` iterator, as well as the `BufferPool.flushAllPages()` method.
The buffer pool stores structs that implement the `Page` interface;  these pages can be read from
underlying database files (such as a heap file) which implement the `DBFile` interface using the
`readPage` method.
The BufferPool should store up to `numPages`
pages. `numPages` is passed to the constructor as a parameter. If more than `numPages` requests are made for different
pages, you should evict one of them according to an eviction policy of your choice (nothing sophisticated needed).
Note that you *should not* evict dirty pages (pages where the `Page` method `isDirty()` returns true), for
reasons we will explain when we discuss transactions later in the class. For Lab 1, if all pages are dirty, return an error.



### Exercise 2

**Implement the `getPage()`, `flushAllPages`, and constructor method in:**

---
* `buffer_pool.go`
---
There is a unit test suite  `buffer_pool_test.go`, but you will not be able to pass this test
until you implement the heap file and heap page methods below.  You will also test the functionality
of the buffer pool when you implement your heap file iterator.

<!--
When more than this many pages are in the buffer pool, one page should be
evicted from the pool before the next is loaded.  The choice of eviction
policy is up to you; it is not necessary to do something sophisticated.
-->

<!--
<p>

Notice that `BufferPool` asks you to implement
a `flush_all_pages()` method.  This is not something you would ever
need in a real implementation of a buffer pool.  However, we need this method
for testing purposes.  You really should never call this method from anywhere
in your code.
-->

### 2.5. `HeapFile` access method

Access methods provide a way to read or write data from disk that is arranged in
a specific way. Common access methods include heap files (unsorted files of
tuples) and B-trees; for this assignment, you will only implement a heap file
access method, and we have written some of the code for you.

A `HeapFile` object is arranged into a set of pages, each of which consists of a
fixed number of bytes for storing tuples, (defined by the constant
`PageSize`), including a header. In GoDB, there is one
`HeapFile` object for each table in the database. Each page in a `HeapFile` is
arranged as a set of slots, each of which can hold one tuple (tuples for a given
table in GoDB are all of the same size).   
Pages of `HeapFile` objects are of type `HeapPage` which
implements the `Page` interface. Pages are stored in the buffer pool but are
read and written by the `HeapFile` class. Because pages are fixed size, and tuple are fixed
size, in GoDB, all pages store the same number of tuples. You are free to choose your
in-memory implementation of `HeapPage` but a reasonable choice would be a slice
of `Tuple`s.  

GoDB stores heap files on disk as pages of data arranged consecutively on
disk. On disk, each page consists of a header, followed
by the `PageSize` - _header size_ bytes of actual page content.
 The header consists of a 32 bit
 integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.   See the comments at the beginning of `heap_page.go` for
more details on the representation.



### Exercise 3

**Implement the skeleton methods in:**

---
* heap_page.go
---

Although you are not required to use exactly our interface for `heap_page.go`,
you will likely find the methods we have provided to be useful and we recommend
following our skeleton.   

Assuming you follow our outline, there are five non-trivial methods to implement:

1. `insertTuple()` : This method should add a tuple to the page if there is space.  Because a heap file is unordered, it
can be inserted in any free slot.  After inserting a tuple on a page, you should mark it dirty.

2. `deleteTuple()` : Delete a specific tuple from the page.
Note that this method takes a specific recordID (or "rid") to delete.  recordID is an empty interface; you are free
to use any struct you like for the rid, but for a heap file a rid would typically include the page number and the slot number on the page.
The page number would typically be the offset in the heap file of the page, and the slot number would likely by the position of the tuple
in the in-memory slice of tuples on the page. You will set the rid field of the tuples you return from your iterator.  Your heap file implementation should use this rid to identify the specific page to delete from, and then pass the rid into this method so that you can delete the appropriate tuple.   Note that if you choose to represent a page in memory as a slice of tuples, and the slot in the rid is the position in the slice, you should take care to not cause the rid to change when you perform the deletion.  One way to achieve this is to set the position in the slice to nil (rather than creating a new slice with the deleted tuple removed from it), but many implementations are possible.  After deleting a tuple from a page, you should mark it dirty.

3. `toBuffer()` : Serialize the pages to a `bytes.Buffer` object for saving to disk, using the `binary.Write()` method to encode the header and the `writeTo()` method from your tuple implementation.   Note that the header includes the number of used slots, but does not encode which slots are empty and which are not.  This is ok, because, in GoDB you do not need to preserve the record ids of records when they are written out (so a particular tuple's rid may change after it is written and then read back.)  

4. `initFromBuffer()` : Read the page from the specified buffer by reading the header with the `binary.Read()` method and then the tuples using the `readTupleFrom()` method.

5. `tupleIter()` : Return a function that can be invoked to interate through the tuples of the page.   See the note about iterators in [2.2](#22-operators-and-iterators) above.

There are a few other relatively simpler methods (`setDirty()`, `isDirty()`, `getNumSlots()`, and the `newHeapPage()` constructor) that you will need to implement

At this point, your code should pass the unit tests in `heap_page_test.go`.

After you have implemented `HeapPage`, you will write methods for `HeapFile` that
read pages from the file, iterate through pages, and insert and delete
records.  

### Exercise 4

**Implement the skeleton methods in:**

---
* heap_file.go
---

There are a number of methods you need to implement; we have provided additional implementation tips in the comments in `heap_file.go`.

1. `NewHeapFile()` - The constructor.  It takes a file name that contains the binary encoding of the file (we name these `table.dat` by convention), as well as the TupleDesc that can be used to determine the expected format of the file and a buffer pool object that you will use to retrieve cached pages.
2. `NumPages()` - Return the number of pages in the heap file;  you can use the `File.Stat()` method to determine the size of the heap file in bytes.  
3. `readPage()` - Read a specific page from storage. To read a page from disk, you will first need to calculate the correct offset in
the file. Hint: you will need random access to the file in order to read and
write pages at arbitrary offsets -- check out the golang `os.File` type and its `ReadAt()` method.
You should not call `BufferPool` methods when reading a page from disk in the `readPage()` method, but you will
use the buffer pool `getPage()` method in your implementations of the heap file `iterator`.  Once you have read in the bytes of the page you can create the page using the heap page method `newHeapPage()`.  You can convert bytes read from a file to a buffer via the `bytes.NewBuffer()` method.
4. `flushPage()` - Force a given page object back to disk.  The supplied page will be a `HeapPage`;  you should cast it and retrieve its bytes via the heap page method `toBuffer()`.  You can then write these bytes back to the appropriate location on disk by opening the backing file and using a method like `os.File.WriteAt()`.
5. `insertTuple()` - Add a tuple to the heap file;  because the heap file is unordered, it can be inserted in any free slot in the file
6. `deleteTuple()` - Remove a specific tuple from the heap file.  You should use the rid field of the tuple to determine which page the
tuple is in, and call the heap page method `deleteTuple()` on the appropriage page.
7. `Descriptor()`
8. `Iterator()` - Return a function that iterates through the tuples of the heap file one at a time.  You should iterate through the pages and use the `tupleIter()` to iterate through the the tuples of each heap page.  See the note above about iterators in GoDB in [2.2](#22-operators-and-iterators) above.
This method should read pages using the buffer pool method `getPage()` which will eventually be used (in
a later lab) to implement locking-based concurrency control and recovery. Do
not load the entire table into memory when the iterator is instantiated -- this will cause an
out of memory error for very large tables.  Instead, you will just load one page at a
time as the buffer pool accesses them via calls to `readPage()`.
9. `pageKey()` - Return a struct that can be used as a key for the page.  The buffer pool uses this to determine whether the page is cached or not.  We have provided an implementation hint in the comment of this function.


At this point, your code should pass the unit tests in `heap_file_test.go` and `buffer_pool_test.go`.  This completes the unit tests for this lab.  You should complete the final exercises in the next section.


<a name="query_walkthrough"></a>

### 2.6. A simple query

In the next lab, you will implement "Operators" that will allow you to run actual SQL queries against GoDB.  For the final test in this lab, we ask you to implement a simple query in go logic.  This method takes the name of a CSV file and a `TupleDesc` and a field name and return the sum of the supplied field name.  You can use the `HeapFile.LoadFromCSV` method to load the CSV file, and the `findFieldInTd` method
to find the field number in the `TupleDesc`, if it exists.

### Exercise 5

**Implement the skeleton method in:**

---
* lab1_query.go
---

We have supplied a simple test case for you for this method in `lab1_query_test.go`, although we will also test it with other files to confirm your implementation is working.

## 3. Logistics

You must submit your code (see below) as well as a short (2 pages, maximum)
writeup describing your approach. This writeup should:

* Describe any design decisions you made. These may be minimal for Lab 1.
* Discuss and justify any changes you made to the API.
* Describe any missing or incomplete elements of your code.
* Describe how long you spent on the lab, and whether there was anything you
  found particularly difficult or confusing.

### 3.1. Collaboration

This lab should be manageable for a single person, but if you prefer to work
with a partner, this is also OK. Larger groups are not allowed. Please indicate
clearly who you worked with, if anyone, on your individual writeup.

### 3.2. Submitting your assignment

<!--
To submit your code, please create a <tt>6.830-lab1.tar.gz</tt> tarball (such
that, untarred, it creates a <tt>6.830-lab1/src/simpledb</tt> directory with
your code) and submit it on the [6.830 Stellar Site](https://stellar.mit.edu/S/course/6/sp13/6.830/index.html). You can use the `ant handin` target to generate the tarball.
-->

We will be using Gradescope to autograde all programming assignments. You should
have all been invited to the class instance; if not, please check Piazza for an
invite code. If you are still having trouble, let us know and we can help you
set up. You may submit your code multiple times before the deadline; we will use
the latest version as determined by Gradescope. Place the write-up in a file
called `lab1-writeup.txt` with your submission.

If you are working with a partner, only one person needs to submit to
Gradescope. However, make sure to add the other person to your group. Also note
that each member must have their own writeup. Please add your Kerberos username
to the file name and in the writeup itself (e.g., `lab1-writeup-username1.txt`
and `lab1-writeup-username2.txt`).

The easiest way to submit to Gradescope is with `.zip` files containing your
code. On Linux/macOS, you can do so by running the following command:

```bash
$ zip -r submission.zip godb/ lab1-writeup.txt

# If you are working with a partner:
$ zip -r submission.zip godb/ lab1-writeup-username1.txt lab1-writeup-username2.txt
```

### 3.3. Submitting a bug

Please submit (friendly!) bug reports to
[6.5830-staff@mit.edu](mailto:6.5830-staff@mit.edu). When you do, please try to
include:

* A description of the bug.
* A `.go` file with test functions that we can drop into the `godb` directory, compile, and run.
* A `.txt` file with the data that reproduces the bug.

If you are the first person to report a particular bug in the code, we will give
you a candy bar!

<!--The latest bug reports/fixes can be found [here](bugs.html).-->

<a name="grading"></a>

### 3.4 Grading


75% of your grade will be based on whether or not your code passes the system
test suite we will run over it. These tests will be a superset of the tests we
have provided. Before handing in your code, you should make sure it produces no
errors (passes all of the tests) when you run `go test` in the `godb` directory.

**Important:** before testing, Gradescope will replace the go test files with our version of these files.
This means you should make sure that your code passes the unmodified tests.

You should get immediate feedback and error outputs for failed visible tests (if any)
from Gradescope after submission. There may exist several hidden tests (a small percentage) that will not be visible until after the deadline.
The score given will be your grade for the
autograded portion of the assignment. An additional 25% of your grade will be
based on the quality of your writeup and our subjective evaluation of your code.
This part will also be published on Gradescope after we finish grading your
assignment.

We had a lot of fun designing this assignment, and we hope you enjoy hacking on
it!
