package grab

import (
	"sync"
)

//WkTok is a work token - Wait for a token before working
type WkTok struct {
	sync.Mutex
	cnt           int
	maxCount      int
	broadcastChan chan struct{}
	waitChan      chan struct{}
}

// NewWkTo creates a work token that you can get and put for
func NewWkTok(cnt int) *WkTok {
	itm := new(WkTok)
	itm.maxCount = cnt
	itm.broadcastChan = make(chan struct{})
	return itm
}
func (wt *WkTok) qTok() bool {
	its := wt.cnt < wt.maxCount
	//fmt.Println("qTok", its)
	return its
}

// Wait for all wokens to complete
func (wt *WkTok) Wait() {
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

// GetTok waits until we get a token
func (wt *WkTok) GetTok() {
	wt.Lock()
	wt.loopToken() // loop until we can increment
	wt.cnt++
	wt.Unlock()
}

// PutTok says we have finished with the token, put it back
func (wt *WkTok) PutTok() {
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

// loopToken loops waiting for a token
func (wt *WkTok) loopToken() {
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
