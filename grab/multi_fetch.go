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
	wg             sync.WaitGroup
	InChan         chan Url // Write Urls to here
	dump_chan      chan Url
	in_chan_closed bool
	scram_chan     chan struct{} // Simply close this channel to scram to filename
	fifo           *UrlStore     // stored here
	ff_map         map[string]*UrlStore
	filename       string
	multi_mode     bool
	download       bool
	counter        int
	fc_lk          sync.Mutex
	st             time.Time
	jpg_tk         *TokenChan
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
	itm.st = time.Now()
	return itm
}
func (mf *MultiFetch) SetTestJpg(cnt int) {
	// Allow up to cnt JPG checks to be happening at once
	mf.jpg_tk = NewTokenChan(cnt, "jpg checker")
}
func (mf *MultiFetch) IncCount() {
	mf.fc_lk.Lock()
	mf.counter++
	mf.fc_lk.Unlock()
}
func (mf *MultiFetch) TotCnt() int {
	return mf.counter + mf.Count()
}
func (mf *MultiFetch) Count() int {
	// Return the number of items we're waiting to fetch
	running_total := mf.fifo.Count()
	for _, v := range mf.ff_map {
		running_total += v.Count()
	}
	return running_total
}
func (mf *MultiFetch) SetFileName(fn string) {
	mf.filename = fn
}
func (mf *MultiFetch) SetDownload() {
	mf.download = true
}
func (mf *MultiFetch) Scram() {
	fmt.Println("Scram Requested", mf.Count())
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
	wt := NewWkTok(4)
	var icd chan Url
	if mf.download {
		// If we are not set to download then
		// leave the cannel as nil so that the
		// read from the channel never occurs
		icd = ic
	}
	for {
		select {
		case <-mf.scram_chan:
			if scram_in_progres == false {
				//fmt.Println("Scram Started")
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
		case urf, ok := <-icd:
			//fmt.Println("Fetch:", urf)
			if !ok {
				// This is a closed channel
				// so all we have to do is wait for
				// any tokens to finish
				wt.Wait()
				return
			}
			basename := urf.Base()
			if basename != "" && dv.VisitedQ(basename) {
				wt.GetTok()
				go func() {
					tchan := make(chan struct{})

					go func() {

						// Fetch returns true if it has used the network
						if mf.FetchW(urf) {
							mf.IncCount()
							time.Sleep(500 * time.Millisecond)
						}
						tchan <- struct{}{}
					}()
					// Implement a timeout on the Fetch Work
					select {
					case <-tchan:
					case <-time.After(20 * time.Minute):
						log.Fatal("We needed to use the timeout case, this is bad!")
					}
					// Return the token at the end
					wt.PutTok()
				}()
			}
		}
	}
}
func (mf *MultiFetch) Worker(dv DomVisitI) {
	mf.wg.Add(1)
	go func() {
		var wg sync.WaitGroup
		if mf.filename != "" {
			wg.Add(1)
			go func() {
        // Because in Multi-Mode re read direct from InChan
        // Maks sure we write there
				LoadGob(mf.filename, mf.InChan, nil, false, false)
				fmt.Println("Finished reading in Gob fully*************************")
				wg.Done()
			}()
		}
		//wg.Wait()

		if !mf.multi_mode {
			mf.single_worker(mf.fifo.PopChannel, dv, "universal")
      wg.Wait()
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
		mf.wg.Done()
	}()
}
func (mf *MultiFetch) Wait() {
	mf.wg.Wait()
}
func (mf *MultiFetch) PrintThroughput() {
	elapsed := time.Since(mf.st).Seconds()
	//fmt.Printf("%d items in %v seconds",mf.counter,elapsed)
	tp := float64(mf.counter) / elapsed
	tp_int := int64(tp)
	fmt.Printf("\n\n%v Media Files at %v Items per Second\n", mf.counter, tp_int)
}

func (mf *MultiFetch) Shutdown() {
	mf.Scram()
	mf.Wait()
	mf.PrintThroughput()
}

func (mf *MultiFetch) scram_multi() {
	SaveGob(mf.filename, mf.dump_chan, nil)
	fmt.Println("Fetch saved")
	//tmp_chan := make(chan Url)
	//LoadGob(mf.filename, tmp_chan, nil, ture false)
	//i:=0
	//for _ = range tmp_chan {
	//  i++
	//}
	//fmt.Println("****Bob File had",i)
}
func (mf *MultiFetch) scram() {
	SaveGob(mf.filename, mf.fifo.PopChannel, nil)
	fmt.Println("Fetch saved")

}
func (mf *MultiFetch) dispatch(dv DomVisitI) {
	// here we read from in chan
	oc := NewOutCounter()
	oc.Add()
	oc.Dec()
  // In Multi-Mode we read Direct from InChan
	for urli := range mf.InChan {
    //fmt.Println("Reading URL in:",urli)
		// work out what the basename of the fetch is
		basename := urli.Base()
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
