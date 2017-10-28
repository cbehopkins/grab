package grab

import (
	"fmt"
	"sync"
)

type outCounter struct {
	sm       sync.Mutex
	Count    int
	closed   bool
	DoneChan chan struct{}
}

// OutCounter is a variant on sync.WaitGroup
// Without the state checking around adding
// while waiting
type OutCounter struct {
	op *outCounter
}

// NewOutCounter creates a new instance of an out counter
func NewOutCounter() *OutCounter {
	itm := new(OutCounter)
	itm.op = new(outCounter)
	itm.initDc()
	return itm
}

// Occer defines the Our Counter interface
type Occer interface {
	Add()
	Dec()
}

// Add a thing to wait on
func (oc OutCounter) Add() {
	//fmt.Println("Add 1 to count")
	oc.op.sm.Lock()
	oc.op.Count++
	if oc.op.closed {
		// We've done an Add();Dec();Add() sequence
		// Where the dec takes count to zero
		// Therefore we need to re-open the channel
		oc.op.DoneChan = nil
		oc.initDc()
	}
	oc.op.sm.Unlock()
}

// Dec that something we were waiting on is done
func (oc OutCounter) Dec() {
	oc.op.sm.Lock()
	if false {
		fmt.Println("Dec 1 from count:", oc.op.Count)
	}
	oc.op.Count--
	if oc.op.Count == 0 {
		oc.initDc()
		oc.op.closed = true
		close(oc.op.DoneChan)
	}
	oc.op.sm.Unlock()
}
func (oc OutCounter) initDc() {
	if oc.op.DoneChan == nil {
		tc := make(chan struct{})
		oc.op.DoneChan = tc
		oc.op.closed = false
	}
}

// Wait for all things to have done
func (oc OutCounter) Wait() {
	oc.initDc()
	<-oc.op.DoneChan
}
