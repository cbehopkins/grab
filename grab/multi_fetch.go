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
	InChan         chan Url // Write Urls to here
	dump_chan      chan Url
	in_chan_closed bool
	scram_chan     chan struct{} // Simply close this channel to scram to filename
	fifo           *UrlStore     // stored here
	ff_map         map[string]*UrlStore
	filename       string
	multi_mode     bool
}

func NewMultiFetch(mm bool) *MultiFetch {
	itm := new(MultiFetch)
	itm.fifo = NewUrlStore()
	itm.ff_map = make(map[string]*UrlStore)
	itm.multi_mode = mm
	if !mm {
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
		mf.in_chan_closed = true
	}
	close(mf.scram_chan)
	mf.Unlock()
}
func (mf *MultiFetch) Close() {
	if !mf.in_chan_closed {
		close(mf.InChan)
		mf.in_chan_closed = true
	}
}

func (mf *MultiFetch) single_worker(ic chan Url, dv DomVisitI, nme string) {

	scram_in_progres := false
	wt := NewWkTok(10)

	for {
		select {
		case <-mf.scram_chan:
			if scram_in_progres == false {
				fmt.Println("Scram Started")
				scram_in_progres = true
				if mf.multi_mode {
					for v := range ic {
						mf.dump_chan <- v
					}
				} else {
					mf.scram()
				}
				fmt.Println("Scram finished for:", nme)
				wt.Wait()
				fmt.Println("worker finished", nme)
				return
			}
		case urf, ok := <-ic:
			fmt.Println("Fetch:", urf)
			if !ok {
				// This is a closed channel
				// so all we have to do is wait for
				// any tokens to finish
				wt.Wait()
				return
			}
			basename := GetBase(string(urf))
			if basename != "" && dv.VisitedQ(basename) {
				wt.GetTok()
				go func() {
					tchan := make(chan struct{})

					go func() {

						// Fetch returns true if it has used the network
						if FetchW(urf, false) {
							time.Sleep(500 * time.Millisecond)
						}
						tchan <- struct{}{}
					}()
					// Implement a timeout on the Fetch Work
					select {
					case <-tchan:
					case <-time.After(10 * time.Minute):
						log.Fatal("We needed to use the timeout case, this is bad!")
					}
					// If not used network then return the token immediatly
					wt.PutTok()
				}()
			}
		}
	}
}
func (mf *MultiFetch) Worker(dv DomVisitI) {
	var wg sync.WaitGroup
	if mf.filename != "" {
		wg.Add(1)
		go func() {
			LoadFile(mf.filename, mf.fifo.PushChannel, nil, false, false)
			wg.Done()
		}()
	}
	wg.Wait()

	if !mf.multi_mode {
		mf.single_worker(mf.fifo.PopChannel, dv, "universal")
	} else {
		mf.dump_chan = make(chan Url)
		// TBD Try this
		// Dispatch will not complete until all of the
		// workers it start complete
		// In the event of a scram a worker will not complete until it has emptied its
		// queue into the queue that is only emptied by scram_multi worker
		go func() {
			<-mf.scram_chan
			wg.Add(1)
			mf.scram_multi()
			wg.Done()
		}()
		mf.dispatch(dv)
		// dispatch workers will finish after they have emptied their queues
		// once they have all emptied then we can close the dump channel
		close(mf.dump_chan)
		fmt.Println("Dispatch Complete - waiting for Scram to finish writing")
		wg.Wait()
	}

}
func (mf *MultiFetch) scram_multi() {
	SaveFile(mf.filename, mf.dump_chan, nil)
	fmt.Println("Fetch saved")
}
func (mf *MultiFetch) scram() {
	SaveFile(mf.filename, mf.fifo.PopChannel, nil)
	fmt.Println("Fetch saved")
}
func (mf *MultiFetch) dispatch(dv DomVisitI) {
	// here we read from in chan
	oc := NewOutCounter()
	oc.Add()
	oc.Dec()
	for urli := range mf.InChan {
		// work out what the basename of the fetch is
		basename := GetBase(string(urli))
		// Check to see if there is an entry for this basename already
		ff, ok := mf.ff_map[basename]
		// Create one if needed
		if !ok {
			oc.Add()
			//fmt.Println("***************Starting*******",basename)
			ff = NewUrlStore()
			mf.ff_map[basename] = ff
			go func() {
				mf.single_worker(ff.PopChannel, dv, basename)
				//fmt.Println("***************Stopping*******",basename)
				oc.Dec()
			}()
		}
		// Send the entry to it
		ff.PushChannel <- urli
	}
	// Now the input has closed
	// Close each of the many queues
	fmt.Println("Dispatch noticed InChan closed")
	for _, ff := range mf.ff_map {
		close(ff.PushChannel)
	}
	// and wait for each of the workers to close
	// This will only happen once their queues are empty
	fmt.Println("Dispatch waiting for all the workers to complete")
	oc.Wait()
	//fmt.Println("All dispatch workers closed")
}
