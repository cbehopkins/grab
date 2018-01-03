package grab

import (
	"sync"
)

type wkTok struct {
	sync.Mutex
	cnt           int
	maxCount      int
	broadcastChan chan struct{}
	waitChan      chan struct{}
}

func newWkTok(cnt int) *wkTok {
	itm := new(wkTok)
	itm.maxCount = cnt
	itm.broadcastChan = make(chan struct{})
	return itm
}
func (wt *wkTok) qTok() bool {
	its := wt.cnt < wt.maxCount
	//fmt.Println("qTok", its)
	return its
}
func (wt *wkTok) wait() {
	wt.Lock()
	if wt.waitChan == nil {
		wt.waitChan = make(chan struct{})
	}
	if wt.cnt == 0 {
		wt.Unlock()
	} else {
		wt.Unlock()
		<-wt.waitChan
	}
}
func (wt *wkTok) getTok() {
	wt.Lock()
	wt.loopToken() // loop until we can increment
	wt.cnt++
	wt.Unlock()
}
func (wt *wkTok) putTok() {
	wt.Lock()
	wt.cnt--
	// one of possibly many receivers will get this
	go func() {
		select {
		case wt.broadcastChan <- struct{}{}:
			//default:
		}
	}()

	defer wt.Unlock()
	if (wt.cnt == 0) && (wt.waitChan != nil) {
		close(wt.waitChan)
		wt.waitChan = nil
	}
}

func (wt *wkTok) loopToken() {
	// Stay in here until the entry does not exist
	for success := wt.qTok(); !success; success = wt.qTok() {
		wt.Unlock()
		// when we get a message
		// There may be many of us waiting
		<-wt.broadcastChan
		// Get the lock
		wt.Lock()
		// and go around again. This time we should succeed
	}
}
