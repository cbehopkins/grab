package grab

import (
	"fmt"
	"sync"
	"time"
)

// Runner is the main starting point for the grab engine
type Runner struct {
	wg sync.WaitGroup
	hm *Hamster

	recycleTime time.Duration
	ust         *uniStore
	grabCloser  chan struct{}
	counter     int
	st          time.Time
	grabSlowly  bool
	pauseLk     sync.Mutex
	pause       bool
	linear      bool
}

// NewRunner - Create a new Runner
func NewRunner(
	chanFetchPush chan<- URL, // Where should we send itmes to fetch
	badURLFn, dir string, // where should we store the bad urls found
	compact, // Should we compact the databases on startup
	promiscuous, // explore everything linked within the domain
	allInteresting, // explore everything linked
	debug, // Print Urls
	polite bool, // resepct robots.txt
) *Runner {
	itm := new(Runner)
	// A DomVisit tracks what domains we're allowed to visit
	// Any domains we come across not in this list will not be visited
	itm.ust = newUniStore(dir, compact, badURLFn)

	// The Hamster is the thing that goes out and
	// Grabs stuff, looking for interesting things to fetch
	itm.hm = NewHamster(
		promiscuous,
		allInteresting,
		debug, // Print Urls
		polite,
	)
	itm.hm.SetDv(itm.ust.getDv())
	itm.hm.SetFetchCh(chanFetchPush)
	itm.recycleTime = 1 * time.Millisecond
	itm.st = time.Now()
	return itm
}

// SeedWg seeds the runner from a specified filename
// returns a sync.WaitGroup to wait on to know it is finished
func (r *Runner) SeedWg(urlFn string, promiscuous bool) *sync.WaitGroup {
	srcURLChan := r.hm.dv.Seed(urlFn, promiscuous)
	wgrc := r.RunChan(srcURLChan, "")
	r.ust.waitLoad()
	return wgrc
}

// LockTest self check function
func (r *Runner) LockTest() {
	r.ust.lockTest()
}

// FlushSync Flush and sync all the databases
func (r *Runner) FlushSync() *sync.WaitGroup {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		r.ust.flushSync()
		wg.Done()
	}()
	return wg
}

// VisitCount returns the count of the number of places we've visited
func (r *Runner) VisitCount() int {
	return r.ust.visitCount()
}

// UnvisitCount returns the count of the nuimber of places we have yet to visit
func (r *Runner) UnvisitCount() int {
	return r.ust.unvisitCount()
}

// RunChan given a channel to read from will add those items to unvisited
// returns a wait group to wait on
// TBD can this be consolidated into the main wait group?
func (r *Runner) RunChan(inputChan <-chan URL, dbgName string) *sync.WaitGroup {
	return r.ust.runChan(inputChan, dbgName)
}

// Wait for everything to finish
func (r *Runner) Wait() {
	//log.Println("Waiting for Runner")
	r.wg.Wait()
	//log.Println("Done Waiting for Runner")
}

// close down the runner
func (r *Runner) close() {
	//if r.grab_closer != nil {
	close(r.grabCloser)
	//}
}

// GoSlow says we should run in slow mode
func (r *Runner) GoSlow() {
	r.grabSlowly = true
}

// PrintThroughput - performance stats
func (r *Runner) PrintThroughput() {
	elapsed := time.Since(r.st).Seconds()
	//fmt.Printf("%d items in %v seconds",r.counter,elapsed)
	tp := float64(r.counter) / elapsed
	tpInt := int64(tp)
	fmt.Printf("\n\n%v Items per Second\n", tpInt)
}

// Resume after a pause
func (r *Runner) Resume() {
	r.pauseLk.Lock()
	if r.pause {
		fmt.Printf("\nResuming Grabber\n\n\n")
	}
	r.pause = false
	r.pauseLk.Unlock()
}

// Pause running until resume called
func (r *Runner) Pause() {
	r.pauseLk.Lock()
	if !r.pause {
		fmt.Printf("\nPausing Grabber\n\n\n")
	}
	r.pause = true
	r.pauseLk.Unlock()
}

// Shutdown the operation
func (r *Runner) Shutdown() {
	var closingWg *sync.WaitGroup
	closingWg = r.FlushSync()

	// We may have paused the runner as part of our throughput limits
	r.Resume()
	r.pauseLk.Lock() // Prevent us pausing again until shutdown
	defer r.pauseLk.Unlock()
	fmt.Println("Close Runneri")
	r.close()
	fmt.Println("Close completer")
	r.Wait() // Once we've closed it make sure it is closeda
	fmt.Println("Waited for runner, closing_wg")
	closingWg.Wait()
	fmt.Println("Close universal store")
	r.ust.closeS()

	fmt.Println("Store closed, printing throughput")
	r.PrintThroughput()
	r.ust.closeDv()
	fmt.Println("Closed Dv")
}

// SetRTime - set the recycle time
func (r *Runner) SetRTime(rt time.Duration) {
	r.recycleTime = rt
}

// GrabRunner launches the grab process with up to numPFetch running at once per domain
func (r *Runner) GrabRunner(numPFetch int) {
	r.wg.Add(1)
	r.grabCloser = make(chan struct{})
	go r.grabRunner(numPFetch)
}

func (r *Runner) grabRunner(numPFetch int) {
	fmt.Println("Starting Hamster")
	defer func() {
		r.wg.Done()
		fmt.Println("grabRunner Complete")
	}()
	defer func() {
		fmt.Println("Closing Hamster")
		r.hm.Close()
		fmt.Println("Hamster Closed")
	}()
	grabTkRep := NewTokenChan(numPFetch, "grab")

	if r.linear {
		r.linGrab(grabTkRep)
	} else {
		r.multiGrab(grabTkRep)
	}
}
func (r *Runner) linGrab(grabTkRep *TokenChan) {
	r.genericMiddle(grabTkRep, r.linGrabMiddle)
}
func (r *Runner) genericMiddle(grabTkRep *TokenChan, midFunc func(grabTkRep *TokenChan, out_count *OutCounter, tmpChan chan URL) bool) bool {
	outCount := NewOutCounter()
	outCount.initDc()
	tmpChan := make(chan URL)
	// Create the worker to sort any new Urls into the  two bins
	wgt := r.ust.runChan(tmpChan, "")

	cc := midFunc(grabTkRep, outCount, tmpChan)
	if UseParallelGrab {
		outCount.Wait()
	}
	fmt.Println("Waiting for runChan to finish adding")
	close(tmpChan)
	wgt.Wait()
	time.Sleep(r.recycleTime)
	return cc
}
func (r *Runner) multiGrab(grabTkRep *TokenChan) {
	var chanClosed bool
	for !chanClosed {
		if r.ust.unvisitSize() <= 0 {
			return
		}
		select {
		case _, ok := <-r.grabCloser:
			if !ok {
				chanClosed = true
				continue
			}
		default:
		}
		r.pauseLk.Lock()
		wePause := r.pause
		r.pauseLk.Unlock()

		if wePause {
			time.Sleep(10 * time.Second)
			continue
		}

		cc := r.genericMiddle(grabTkRep, r.multiGrabMiddle)
		if cc {
			chanClosed = true
		}

	}
}
func (r *Runner) linGrabMiddle(grabTkRep *TokenChan, outCount *OutCounter, tmpChan chan URL) bool {
	grabSlowly := r.grabSlowly
	var chanClosed bool
	somethingSkipped := true
	var lastURL URL
	for somethingSkipped {
		var delList []URL
		somethingSkipped = false
		urlChan := r.ust.VisitFrom(lastURL)
		chanClosed = false
		for !chanClosed {
			select {
			case _, ok := <-r.grabCloser:
				if !ok {
					somethingSkipped = false
					chanClosed = true
				}
			case urv, ok := <-urlChan:
				if !ok {
					chanClosed = true
				} else {
					if r.hm.grabItWork(urv, outCount, grabTkRep, tmpChan) {
						delList = append(delList, urv)
						// while we are using Visit, not VisitAll
						somethingSkipped = true
					} else {
						lastURL = urv
						somethingSkipped = true
					}
				}
			}
		}
		for _, urs := range delList {
			r.ust.setVisited(urs)
		}
		if somethingSkipped {
			if grabSlowly {
				time.Sleep(10 * time.Second)
			} else {
				// Give a Ping time for Grabbers to work
				// Yes it's not enough, but after a couple of itterations we should
				// get a nice balanced system
				time.Sleep(10 * time.Millisecond)
			}
		} else if !chanClosed && lastURL.URL() != "" {
			// It's possible lastURL is set to the last thing in the array
			// but that there are still lots of things to visit
			// In which case we should reset to the beginning
			somethingSkipped = true
			lastURL = NewURL("")
		}
		if chanClosed {
			return true
		}
	}
	return false
}
func (r *Runner) multiGrabMiddle(grabTkRep *TokenChan, outCount *OutCounter, tmpChan chan URL) bool {
	var chanClosed bool
	missingMapString := r.ust.getMissing(grabTkRep)
	r.ust.flushSync()
	//fmt.Println("Missing string rxd")
	// Convert into a map of urls rather than string
	missingMap := make(map[URL]struct{})
	for urv := range missingMapString {
		newURL := r.ust.retrieveUnvisted(urv)
		if newURL != nil {
			missingMap[*newURL] = struct{}{}
		}
	}
	fmt.Printf("Runnin iter loop with %v\n", len(missingMap))
	for iterCnt := 0; !chanClosed && (iterCnt < 100) && (len(missingMap) > 0); iterCnt++ {
		closeR := r.runMultiGrabMap(missingMap, outCount, grabTkRep, tmpChan)
		if closeR {
			return true
		}
	}
	return false
}
func (r *Runner) runMultiGrabMap(missingMap map[URL]struct{}, outCount *OutCounter, grabTkRep *TokenChan, tmpChan chan URL) bool {
	grabSuccess, closeChan := r.workMultiMap(missingMap, outCount, grabTkRep, tmpChan)
	grabSlowly := r.grabSlowly
	for _, urv := range grabSuccess {
		r.counter++
		delete(missingMap, urv)
	}
	if closeChan {
		return true
	}
	if grabSlowly {
		time.Sleep(10 * time.Second)
	} else {
		// Give a Ping time for Grabbers to work
		// Yes it's not enough, but after a couple of itterations we should
		// get a nice balanced system
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
func (r *Runner) workMultiMap(missingMap map[URL]struct{}, outCount *OutCounter, grabTkRep *TokenChan, tmpChan chan URL) (grabSuccess []URL, closeChan bool) {
	grabSuccess = make([]URL, 0, len(missingMap))
	for urv := range missingMap {
		urv.Base()
		select {
		case _, ok := <-r.grabCloser:
			if !ok {
				return grabSuccess, true
			}
		default:
			if r.getConditional(urv, outCount, grabTkRep, tmpChan) {
				// If we sucessfully grab this (get a token etc)
				// then delete it fro the store
				//One we haven't visited we need to run a grab on
				// This fetch the URL and look for what to do
				grabSuccess = append(grabSuccess, urv)
			}
		}
	}
	return grabSuccess, false
}

// AutoPace - automatically pause and resume runner
// When activated pace the runner.
// when there is more than target things to fetch. Pause the runner
// then less, then activate it
func (r *Runner) AutoPace(multiFetch *MultiFetch, target int) {
	runAuto := true
	for current := multiFetch.Count(); runAuto; current = multiFetch.Count() {
		select {
		case _, ok := <-r.grabCloser:
			if !ok {
				runAuto = false
			}
		default:
			if current > target {
				r.Pause()
				time.Sleep(10 * time.Second)
			} else {
				r.Resume()
				time.Sleep(1 * time.Second)
			}
		}
	}
}
func (r *Runner) getConditional(urs URL, outCount *OutCounter, crawlChan *TokenChan, tmpChan chan<- URL) bool {
	if r.hm.grabItWork(urs, outCount, crawlChan, tmpChan) {
		r.ust.setVisited(urs)
		return true
	}
	return false
}

// Dump the databases ount into human readable format
func (r *Runner) Dump(unvisitFilename, visitFilename string) {
	if unvisitFilename != "" {
		r.ust.dumpUnvisited(unvisitFilename)
	}
	if visitFilename != "" {
		r.ust.dumpVisited(visitFilename)
	}
}

// Seed the databases from a file
func (r *Runner) Seed(urlFn string, promiscuous bool) chan URL {
	return r.ust.Seed(urlFn, promiscuous)
}

// ClearVisited move all visited locations into unvisited
func (r *Runner) ClearVisited() {
	r.ust.clearVisited()
}

// GoodURL returnhs true if a url is Good (probably)
func (r *Runner) GoodURL(urlIn URL) bool {
	return r.ust.GoodURL(urlIn)
}

// VisitedQ - Question: have we visited this?
func (r *Runner) VisitedQ(urlIn string) bool {
	return r.ust.VisitedQ(urlIn)
}

// VisitedA - Add if we haven't visited it
func (r *Runner) VisitedA(urlIn string) bool {
	return r.ust.VisitedA(urlIn)
}

// SetLinear - activates linear fetch mode
func (r *Runner) SetLinear() {
	r.linear = true
}

// ClearLinear - deactivate linear fetch mode
func (r *Runner) ClearLinear() {
	r.linear = false
}
