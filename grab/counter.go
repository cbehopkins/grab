package grab

import (
	"sync"
)

type OutCounter struct {
	sync.Mutex
	Count    int
	DoneChan chan struct{}
}

func (oc *OutCounter) Add() {
	oc.Lock()
	oc.Count++
	oc.Unlock()
}
func (oc *OutCounter) Dec() {
	oc.Lock()
	oc.Count--
	oc.Unlock()
	if oc.Count == 0 {
		close(oc.DoneChan)
	}
}
func (oc *OutCounter) Wait() {
	if oc.DoneChan == nil {
		oc.DoneChan = make(chan struct{})
	}
	<-oc.DoneChan
}
