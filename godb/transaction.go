package godb

import "sync"

type TransactionID int

var nextTid = 0
var newTidMutex sync.Mutex

func NewTID() TransactionID {
	newTidMutex.Lock()
	defer newTidMutex.Unlock()
	id := nextTid
	nextTid++
	return TransactionID(id)
}

//var tid TransactionID = NewTID()
