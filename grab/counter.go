package grab

import (
	"sync"
	"fmt"
)

type OutCounter struct {
	sync.Mutex
	Count    int
	closed	 bool
	DoneChan chan struct{}
}

func (oc *OutCounter) Add() {
        //fmt.Println("Add 1 to count")
	oc.Lock()
	oc.Count++
	if oc.closed {
		// We've done an Add();Dec();Add() sequence
		// Where the dec takes count to zero
		// Therefore we need to re-open the channel
		oc.DoneChan = make(chan struct{})
		oc.closed = false
	}
	oc.Unlock()
}
func (oc *OutCounter) Dec() {
	oc.Lock()
        if false {fmt.Println("Dec 1 from count:", oc.Count)}
	oc.Count--
	if oc.Count == 0 {
	 	if oc.DoneChan == nil {
                	oc.DoneChan = make(chan struct{})
	        }
		oc.closed = true
		close(oc.DoneChan)
	}
	oc.Unlock()
}
func (oc *OutCounter) Wait() {
	if oc.DoneChan == nil {
		oc.DoneChan = make(chan struct{})
	}
	<-oc.DoneChan
}
