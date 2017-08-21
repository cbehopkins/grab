package grab

import (
	"sync"
)

type wkTok struct {
	sync.Mutex
	cnt            int
	max_count      int
	broadcast_chan chan struct{}
	wait_chan      chan struct{}
}

func NewWkTok(cnt int) *wkTok {
	itm := new(wkTok)
	itm.max_count = cnt
	itm.broadcast_chan = make(chan struct{})
	return itm
}
func (wt *wkTok) qTok() bool {
	its := wt.cnt < wt.max_count
	//fmt.Println("qTok", its)
	return its
}
func (wt *wkTok) Wait() {
	wt.Lock()
	if wt.wait_chan == nil {
		wt.wait_chan = make(chan struct{})
	}
	if wt.cnt == 0 {
		wt.Unlock()
	} else {
		wt.Unlock()
		<-wt.wait_chan
	}
}
func (wt *wkTok) GetTok() {
	wt.Lock()
	wt.loopToken() // loop until we can increment
	wt.cnt++
	wt.Unlock()
}
func (wt *wkTok) PutTok() {
	wt.Lock()
	wt.cnt--
	// one of possibly many receivers will get this
	select {
	case wt.broadcast_chan <- struct{}{}:
	default:
	}

	defer wt.Unlock()
	if (wt.cnt == 0) && (wt.wait_chan != nil) {
		close(wt.wait_chan)
	}
}

func (wt *wkTok) loopToken() {
	// Stay in here until the entry does not exist
	for success := wt.qTok(); !success; success = wt.qTok() {
		wt.Unlock()
		// when we get a message
		// There may be many of us waiting
		<-wt.broadcast_chan
		// Get the lock
		wt.Lock()
		// and go around again. This time we should succeed
	}
}
