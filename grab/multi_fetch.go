package grab

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// One multi-Fetcher does all files!
type MultiFetch struct {
	sync.Mutex
	InChan     chan Url      // Write Urls to here
	in_chan_closed bool
	scram_chan chan struct{} // Simply close this channel to scram to filename
	fifo       *UrlStore     // stored here
	ff_map     map[string]*UrlStore
	filename   string
	
}

func NewMultiFetch() *MultiFetch {
	itm := new(MultiFetch)
	itm.fifo = NewUrlStore()
	itm.ff_map = make(map[string]*UrlStore)
	if true {
		itm.InChan = itm.fifo.PushChannel
	} else {
		itm.InChan = make(chan Url)
	}
	itm.scram_chan = make(chan struct{})
	return itm
}

func (mf *MultiFetch) SetFileName(fn string) {
	mf.filename = fn
}

func (mf *MultiFetch) Scram() {
	fmt.Println("Scram Requested")
	mf.Lock()
	if !mf.in_chan_closed {
	close(mf.InChan)
	mf.in_chan_closed =  true
	}
	close(mf.scram_chan)
	mf.Unlock()
}
func (mf *MultiFetch) Close() {
	if !mf.in_chan_closed {
	close(mf.InChan)
mf.in_chan_closed =  true
        }
}

func (mf *MultiFetch) single_worker(ic chan Url) {

	scram_in_progres := false
	wt := NewWkTok(2)

	for {
		select {
		case <-mf.scram_chan:
			if scram_in_progres == false {
				fmt.Println("Scram Started")
				scram_in_progres = true
				mf.scram()
				wt.Wait()
				return
			}
		case urf, ok := <-ic:
			fmt.Println("Fetch:", urf)
			if !ok {
				// This is a closed channel
				// so all we have to do is wait for
				// any tokens to finish
				return
			}
			wt.GetTok()
			tchan := make (chan struct{})
			go func() {
				// Fetch returns true if it has used the network
				if FetchW(urf, false) {
					time.Sleep(500 * time.Millisecond)
				}
				tchan <- struct{}{}
			}()
				// Implement a timeout on the Fetch Work
				select {
					case <- tchan:
					case <- time.After(10*time.Second):
						log.Fatal("We needed to use the timeout case, this is bad!")
				}
				// If not used network then return the token immediatly
				wt.PutTok()
		}
	}
	wt.Wait()
}
func (mf *MultiFetch) Worker(filename string) {
	var wg sync.WaitGroup
	if filename != "" {
		wg.Add(1)
		go func() {
			LoadFile(filename, mf.fifo.PushChannel, nil, false, false)
			wg.Done()
		}()
	}
	wg.Wait()

	if true {
		mf.single_worker(mf.fifo.PopChannel)
	} else {
		// TBD Try this
		mf.dispatch()
	}

}
func (mf *MultiFetch) scram() {
	SaveFile(mf.filename, mf.fifo.PopChannel, nil)
	fmt.Println("Fetch saved")
}
func (mf *MultiFetch) dispatch() {
	// here we read from in chan
	oc := NewOutCounter()
	for urli := range mf.InChan {
		// work out what the basename of the fetch is
		basename := GetBase(string(urli))
		// Check to see if there is an entry for this basename already
		ff, ok := mf.ff_map[basename]
		// Create one if needed
		if !ok {
			oc.Add()
			ff = NewUrlStore()
			mf.ff_map[basename] = ff
			go func() {
				mf.single_worker(ff.PopChannel)
				oc.Dec()
			}()
		}
		// Send the entry to it
		ff.PushChannel <- urli
	}
	oc.Wait()
}
