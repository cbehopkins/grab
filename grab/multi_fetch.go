package grab

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// MultiFetch is a parallel fetch engine
type MultiFetch struct {
	// We ar protected by a simple mutex
	sync.Mutex
	wg sync.WaitGroup
	// Gob Wait Group
	// Ensure we Wait for gob to have finished before we close the fetch channel
	// That gob will be trying to write to
	gwg       sync.WaitGroup
	InChan    chan URL // Write Urls to here
	dumpChan  chan URL
	scramChan chan struct{} // Simply close this channel to saveProgress to filename
	closeChan chan struct{}
	fifo      *URLStore // stored here
	ffMap     map[string]*URLStore
	filename  string
	multiMode bool
	download  bool
	debug     bool
	counter   int
	fcLk      sync.Mutex
	st        time.Time
	jpgTk     *TokenChan
}

// NewMultiFetch - create a new fetch engine - specify if it should use multi mode
func NewMultiFetch(mm bool) *MultiFetch {
	itm := new(MultiFetch)
	itm.ffMap = make(map[string]*URLStore)
	itm.multiMode = mm
	if mm {
		itm.InChan = *NewURLChannel()
	} else {
		itm.InChan = itm.fifo.PushChannel
		itm.fifo = NewURLStore()
	}
	itm.scramChan = make(chan struct{})
	itm.closeChan = make(chan struct{})
	itm.st = time.Now()
	return itm
}

// SetTestJpg Allow up to cnt JPG checks to be happening at once
// too small and you're wasting cpu potential
// too high and you're thrashing the file system
func (mf *MultiFetch) SetTestJpg(cnt int) {
	mf.jpgTk = NewTokenChan(cnt, "jpg checker")
}

// SetDebug turn on the debug mode
func (mf *MultiFetch) SetDebug() {
	mf.debug = true
}

// Keep tack of the nuber that are running
func (mf *MultiFetch) incCount() {
	mf.fcLk.Lock()
	mf.counter++
	mf.fcLk.Unlock()
}

// TotCnt includes the ones we've completed
func (mf *MultiFetch) TotCnt() int {
	return mf.counter + mf.Count()
}

// Count returns the number of items we're waiting to fetch
func (mf *MultiFetch) Count() int {
	var runningTotal int
	if mf.fifo != nil {
		runningTotal = mf.fifo.Count()
	}
	for _, v := range mf.ffMap {
		runningTotal += v.Count()
	}
	return runningTotal
}

// SetFileName of the file to read in history from
func (mf *MultiFetch) SetFileName(fn string) {
	mf.filename = fn
	mf.gwg.Add(1)
	go func() {
		// Because in Multi-Mode re read direct from InChan
		// Maks sure we write there
		LoadGob(mf.filename, mf.InChan, nil, false)
		fmt.Println("Finished reading in Gob fully*************************")
		mf.gwg.Done()
	}()
}

// SetDownload marks that we should download items rather than just accrue them
func (mf *MultiFetch) SetDownload() {
	mf.download = true
}

// Scram means stop the download process now
// I guess Close would be a more common word
// Ther's an overlap of functions here - we could tidy this up
// But I like the idea of saveProgress!
func (mf *MultiFetch) Scram() {
	//fmt.Println("Scram Requested", mf.Count())
	if !mf.Scraming() {
		close(mf.scramChan)
	}
}

// Closed returns true if the mf is closed for business
func (mf *MultiFetch) Closed() bool {
	select {
	case <-mf.closeChan:
		return true
	default:
		return false
	}
}

// Close down the multi fetcher
// Close does not scram, it waits for downloads to finish
func (mf *MultiFetch) Close() {
	// Wait for the Load gob to finish
	mf.gwg.Wait()
	mf.close()
}

// GobWait waits for the gob to finish loading
func (mf *MultiFetch) GobWait() {
	mf.gwg.Wait()
}
func (mf *MultiFetch) close() {
	if !mf.Closed() {
		close(mf.closeChan)
	}
}

// Scraming returns true if we are in the process of scramming the mf
func (mf *MultiFetch) Scraming() bool {
	select {
	case <-mf.scramChan:
		return true
	default:
		return false
	}
}
func (mf *MultiFetch) workScram(ic chan URL, dv DomVisitI, nme string, scramInProgres *bool, wt *WkTok) {
	if *scramInProgres == false {
		//fmt.Println("Scram Started")
		*scramInProgres = true
		if mf.multiMode {
			for v := range ic {
				mf.dumpChan <- v
			}
		} else {
			mf.saveProgress()
		}
		fmt.Println("Scram finished for:", nme)
		wt.Wait()
		fmt.Println("worker finished", nme)
		return
	}
}
func (mf *MultiFetch) singleWorker(ic chan URL, dv DomVisitI, nme string) {
	scramInProgres := false
	wt := NewWkTok(4)
	var icd chan URL
	if mf.download {
		// If we are not set to download then
		// leave the cannel as nil so that the
		// read from the channel never occurs
		icd = ic
	}
	for {
		select {
		case <-mf.scramChan:
			mf.workScram(ic, dv, nme, &scramInProgres, wt)
		case urf, ok := <-icd:
			if !ok {
				// This is a closed channel
				// so all we have to do is Wait for
				// any tokens to finish
				fmt.Println("singleWorker seen closed", nme)
				wt.Wait()
				return
			}
			//fmt.Println("Fetch:", urf)
			if urf.base == nil {
				urf.Initialise()
			}
			basename := urf.Base()
			if basename != "" && dv.VisitedQ(basename) {
				wt.GetTok()
				if mf.Scraming() {
					// return the urf
					mf.dumpChan <- urf
					wt.PutTok()
				} else {
					go func() {
						df := func() { mf.downFunc(urf) }
						withTimeout(df, "Timeout of url:"+urf.String())
						wt.PutTok()
					}()
				}
			}
		}
	}
}
func (mf *MultiFetch) downFunc(urg URL) {
	// Fetch returns true if it has used the network
	if mf.fetchW(urg) {
		//fmt.Println("Fetch Complete:", urf)
		mf.incCount()
		time.Sleep(500 * time.Millisecond)
	}
}

// Worker starts the worker using the domvisit tracker specified
// This is normally contained in the Runner struct
// so as long as that had exported the interface we're fine
func (mf *MultiFetch) Worker(dv DomVisitI) {
	mf.wg.Add(1)
	go func() {

		if !mf.multiMode {
			mf.singleWorker(mf.fifo.PopChannel, dv, "universal")
		} else {
			log.Println("Multi mode worker entered")
			var wg sync.WaitGroup
			mf.dumpChan = make(chan URL)
			// Dispatch will not complete until all of the
			// workers it start complete
			// In the event of a saveProgress a worker will not complete until it has emptied its
			// queue into the queue that is only emptied by scram_multi worker
			go func() {
				<-mf.scramChan
				wg.Add(1)
				fmt.Println("MultiScram Start")
				mf.scramMulti()
				fmt.Println("Multiscram End")
				// We can't shutdown the BuffCache until
				// we are sure no more fetches will start
				// This is true once the mf inputs are drained
				BuffCache.Close()
				fmt.Println("BuffCache Written")
				wg.Done()
			}()
			mf.dispatch(dv)
			fmt.Println("Dispatch Complete - Closing starting")
			// dispatch workers will finish after they have emptied their queues
			// once they have all emptied then we can close the dump channel
			close(mf.dumpChan)
			fmt.Println("Dispatch Complete - waiting for Scram to finish writing")
			wg.Wait()
		}
		mf.wg.Done()
	}()
}

// Wait for the process to complete
func (mf *MultiFetch) Wait() {
	//if mf.debug {
	fmt.Println("MultiFetch wait", mf.Closed())
	//}
	mf.wg.Wait()
	//if mf.debug {
	fmt.Println("MultiFetch wait complete")
	//}
}

// PrintThroughput gives us output stats
func (mf *MultiFetch) PrintThroughput() {
	elapsed := time.Since(mf.st).Seconds()
	fmt.Printf("\n\n%v Media Files at %s", mf.counter, Throughput(mf.counter, elapsed))
}

// Shutdown very similar to Close/Scram
// TBD Why do we have this?
func (mf *MultiFetch) Shutdown() {
	mf.Wait()
	mf.Close()
	mf.PrintThroughput()
}

func (mf *MultiFetch) scramMulti() {
	SaveGob(mf.filename, mf.dumpChan, nil)
	fmt.Println("Fetch saved")
}
func (mf *MultiFetch) saveProgress() {
	SaveGob(mf.filename, mf.fifo.PopChannel, nil)
	fmt.Println("Fetch saved")

}
func (mf *MultiFetch) dispatch(dv DomVisitI) {
	// here we read from in chan
	//oc := NewOutCounter()
	var oc sync.WaitGroup
	//log.Println("dispatch entered")
	for urli := range mf.InChan {
		//fmt.Println("Reading URL in:", urli)
		// work out what the basename of the fetch is
		basename := urli.Base()
		// Check to see if there is an entry for this basename already
		ff, ok := mf.ffMap[basename]
		// Create one if needed
		if !ok {
			oc.Add(1)
			//fmt.Println("***************Starting*******", basename)
			ff = NewURLStore()
			mf.ffMap[basename] = ff
			go func() {
				mf.singleWorker(ff.PopChannel, dv, basename)
				//fmt.Println("***************Stopping*******", basename)
				oc.Done()
			}()
		}
		// Send the entry to it
		ff.PushChannel <- urli
	}
	// Now the input has closed
	// Close each of the many queues
	fmt.Println("Dispatch noticed InChan closed")
	for _, ff := range mf.ffMap {
		close(ff.PushChannel)
	}
	// and Wait for each of the workers to close
	// This will only happen once their queues are empty
	fmt.Println("Dispatch waiting for all the workers to complete")
	oc.Wait()
	//fmt.Println("All dispatch workers closed")
}

func (mf *MultiFetch) fetchW(fetchURL URL) bool {
	if mf.Closed() {
		return false
	}
	// We retun true if we have used network bandwidth.
	// If we have not then it's okay to jump straight onto the next file
	array := strings.Split(fetchURL.URL(), "/")

	var fn string
	if len(array) > 2 {
		fn = array[len(array)-1]
	} else {
		return false
	}
	if strings.HasPrefix(fetchURL.URL(), "file") {
		return false
	}

	fn = strings.TrimLeft(fn, ".php?")
	// logically there must be http:// so therefore length>2
	dirStruct := array[2 : len(array)-1]
	dirStr := strings.Join(dirStruct, "/")
	dirStr = strings.Replace(dirStr, "//", "/", -1)
	dirStr = strings.Replace(dirStr, "%", "_", -1)
	dirStr = strings.Replace(dirStr, "&", "_", -1)
	dirStr = strings.Replace(dirStr, "?", "_", -1)
	dirStr = strings.Replace(dirStr, "=", "_", -1)

	re := regexp.MustCompile("(.*\\.jpg)(.*)")
	t1 := re.FindStringSubmatch(fn)
	if len(t1) > 1 {
		fn = t1[1]
	}
	pageTitle := fetchURL.GetTitle()
	if pageTitle != "" {
		pageTitle = strings.Replace(pageTitle, "/", "_", -1)
		if strings.HasSuffix(fn, ".mp4") {
			fn = pageTitle + ".mp4"
			//fmt.Println("Title set, so set filename to:", fn)
		}
	}
	fn = strings.Replace(fn, "%", "_", -1)
	fn = strings.Replace(fn, "&", "_", -1)
	fn = strings.Replace(fn, "?", "_", -1)
	fn = strings.Replace(fn, "=", "_", -1)
	fn = strings.Replace(fn, "\"", "_", -1)
	fn = strings.Replace(fn, "'", "_", -1)
	fn = strings.Replace(fn, "!", "_", -1)
	fn = strings.Replace(fn, ",", "_", -1)
	potentialFileName := dirStr + "/" + fn
	if strings.HasPrefix(potentialFileName, "/") {
		return false
	}
	if _, err := os.Stat(potentialFileName); !os.IsNotExist(err) {
		// For a file that does already exist
		if mf.jpgTk == nil {
			// We're not testing all the jpgs for goodness
			//fmt.Println("skipping downloading", potential_file_name)
			return false
		} else if strings.HasSuffix(fn, ".jpg") {
			// Check if it is a corrupted file. If it is, then fetch again
			//fmt.Println("yest jph", fn)
			mf.jpgTk.GetToken("jpg")
			goodFile := checkJpg(potentialFileName)
			mf.jpgTk.PutToken("jpg")
			if goodFile {
				return false
			}
		} else {
			// not a jpg and it already exists
			return false
		}
	}

	// For a file that doesn't already exist, then just fetch it
	if mf.debug {
		fmt.Printf("Fetching %s, fn:%s\n", fetchURL, potentialFileName)
	}

	// if fetchURL does not have a recognised ending
	// look through the url for one
	// and add that to the title
	replacePfn, extension := findExtension(fetchURL.URLs, potentialFileName)
	if replacePfn {
		potentialFileName = dirStr + "/" + fetchURL.Title + "_rewrite." + extension
		if mf.debug {
			fmt.Println("Rewrite filename to:", potentialFileName)
		}
	}
	//fmt.Println("Fetchfile start", potentialFileName)

	fetchFile(potentialFileName, dirStr, fetchURL)
	//fmt.Println("Fetchfile complete", potentialFileName)
	return true
}
func findExtension(fetchURL, potentialFileName string) (bool, string) {
	possibleExtensions := []string{"jpg", "mp4", "flv"}
	for _, poss := range possibleExtensions {
		if strings.HasSuffix(potentialFileName, poss) {
			return false, potentialFileName
		}
	}
	for _, poss := range possibleExtensions {
		if strings.Contains(fetchURL, poss) {
			return true, poss
		}
	}
	return false, potentialFileName
}
