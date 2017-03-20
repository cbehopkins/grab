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
type OutCounter struct {
	op *outCounter
}

func NewOutCounter() *OutCounter {
	itm := new(OutCounter)
	itm.op = new(outCounter)
	return itm
}

type Occer interface {
	Add()
	Dec()
}

func (oc OutCounter) Add() {
	//fmt.Println("Add 1 to count")
	oc.op.sm.Lock()
	oc.op.Count++
	if oc.op.closed {
		// We've done an Add();Dec();Add() sequence
		// Where the dec takes count to zero
		// Therefore we need to re-open the channel
		tc := make(chan struct{})
		oc.op.DoneChan = tc
		oc.op.closed = false
	}
	oc.op.sm.Unlock()
}

func (oc OutCounter) Dec() {
	oc.op.sm.Lock()
	if false {
		fmt.Println("Dec 1 from count:", oc.op.Count)
	}
	oc.op.Count--
	if oc.op.Count == 0 {
		if oc.op.DoneChan == nil {
			tc := make(chan struct{})
			oc.op.DoneChan = tc
		}
		oc.op.closed = true
		close(oc.op.DoneChan)
	}
	oc.op.sm.Unlock()
}
func (oc OutCounter) Wait() {
	if oc.op.DoneChan == nil {
		tc := make(chan struct{})
		oc.op.DoneChan = tc
	}
	<-oc.op.DoneChan
}
