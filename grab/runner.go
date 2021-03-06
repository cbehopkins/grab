package grab

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cbehopkins/token"
)

// Runner is the main starting point for the grab engine
type Runner struct {
	wg sync.WaitGroup
	hm *Hamster

	recycleTime    time.Duration
	ust            *uniStore
	grabCloser     chan struct{}
	counter        int
	st             time.Time
	grabSlowly     bool
	pauseLk        sync.Mutex
	pause          bool
	debug          bool
	allInteresting bool
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
	itm.debug = debug
	itm.allInteresting = allInteresting
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

// Close down the runner
func (r *Runner) Close() {
	if !r.closed() {
		close(r.grabCloser)
	}
}

// GoSlow says we should run in slow mode
func (r *Runner) GoSlow() {
	r.grabSlowly = true
}

// PrintThroughput - performance stats
func (r *Runner) PrintThroughput() {
	elapsed := time.Since(r.st).Seconds()
	fmt.Print("\n\n", Throughput(r.counter, elapsed))
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
	r.Close()
	fmt.Println("Close completer")
	r.wg.Add(1)
	r.wg.Done()
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

type mf func(*token.MultiToken, *OutCounter, chan<- URL) bool

func (r *Runner) grabRunner(numPFetch int) {
	//fmt.Println("Starting Hamster")
	defer func() {
		fmt.Println("Closing Hamster")
		r.hm.Close()
		fmt.Println("Hamster Closed")
		r.wg.Done()
		fmt.Println("grabRunner Complete")
	}()
	grabTkRep := token.NewTokenChan(numPFetch, "grab")

	r.genericOuter(grabTkRep, r.multiGrabMiddle)
}

func (r *Runner) genericMiddle(grabTkRep *token.MultiToken, midFunc mf) bool {
	if r.debug {
		fmt.Println("genericMiddle starting")
	}
	outCount := NewOutCounter()
	// pretend we have some workload so that we don't lock on zero work
	//outCount.Add()
	//outCount.Dec()
	tmpChan := make(chan URL)
	// Create the worker to sort any new Urls into the  two bins
	// i.e. tmpChan receives URLs from the crawler and runChan sorts into visited and unvisited
	wgt := r.ust.runChan(tmpChan, "")

	cc := midFunc(grabTkRep, outCount, tmpChan)
	if r.debug {
		fmt.Println("midFunc complete", cc)
	}
	if UseParallelGrab {
		if r.debug {
			fmt.Println("Wait for outCount in genericMiddle")
		}
		outCount.Wait()
	}
	close(tmpChan)
	if r.debug {
		fmt.Println("Waiting for runChan to finish adding")
	}
	wgt.Wait()
	r.abortableSleep(r.recycleTime)
	return cc
}

// Cycle returns true if we are closed
// and therefore should stop doing what you're doing
// makes a convenient loop variable
// in that we pause if we should, otherwise we go for it
func (r *Runner) cycle() bool {
	wePause := true
	for wePause {
		if r.closed() {
			if r.debug {
				fmt.Println("cycle detected closed")
			}
			return true
		}
		// We don't expect to have multiple threads contending for
		// read access to this lock
		// so KISS
		r.pauseLk.Lock()
		wePause = r.pause
		r.pauseLk.Unlock()

		if wePause {
			r.abortableSleep(10 * time.Second)
		} else {
			return r.closed()
		}
	}
	return false
}
func (r *Runner) abortableSleep(t time.Duration) bool {
	return abortableSleep(t, r.closed)
}

func (r *Runner) genericOuter(grabTkRep *token.MultiToken, midFunc mf) bool {
	var chanClosed bool
	if r.debug {
		fmt.Println("genericOuter starting")
		defer fmt.Println("genericOuter complete", chanClosed)
	}
	for !chanClosed {
		if r.ust.unvisitSize() <= 0 {
			return true
		}
		if r.cycle() {
			return chanClosed
		}
		chanClosed = r.genericMiddle(grabTkRep, midFunc)
		if r.debug {
			fmt.Println("r.genericMiddle complete", chanClosed)
		}
	}
	return chanClosed
}

// Sleep for as long as we should given the mode we are running
func (r *Runner) Sleep() {
	if r.closed() {
		return
	} else if r.grabSlowly {
		r.abortableSleep(10 * time.Second)
	} else {
		// Set to a fast ping time
		time.Sleep(10 * time.Millisecond)
	}
}
func (r *Runner) closed() bool {
	select {
	case _, ok := <-r.grabCloser:
		if !ok {
			return true
		}
	default:
	}
	return false
}

func (r *Runner) multiGrabMiddle(grabTkRep *token.MultiToken, outCount *OutCounter, tmpChan chan<- URL) bool {
	if r.debug {
		log.Println("Multi is starting - getMissing started")
	}
	missingMapString := r.ust.getMissing(grabTkRep)
	if r.debug {
		log.Println("getMissing returned")
	}
	r.ust.flushSync()
	// Convert into a map of urls rather than string
	//log.Println("Building Map of where to visit")
	missingMap := make(map[URL]struct{})
	for urv := range missingMapString {
		newURL := r.ust.retrieveUnvisted(urv)
		if newURL != nil {
			missingMap[*newURL] = struct{}{}
		}
	}
	if r.debug {
		log.Printf("Runnin iter loop with %v\n", len(missingMap))
	}
	for iterCnt := 0; (iterCnt < 100) && (len(missingMap) > 0); iterCnt++ {
		closeR := r.runMultiGrabMap(missingMap, outCount, grabTkRep, tmpChan)
		if closeR {
			if r.debug {
				fmt.Println("closing multiMiddle")
			}
			return true
		}
	}
	return false
}
func (r *Runner) runMultiGrabMap(missingMap map[URL]struct{}, outCount *OutCounter, grabTkRep *token.MultiToken, tmpChan chan<- URL) bool {
	grabSuccess, closeChan := r.workMultiMap(missingMap, outCount, grabTkRep, tmpChan)
	//if r.debug {
	//	log.Println("r.workMultiMap complete", closeChan)
	//}
	for _, urv := range grabSuccess {
		r.counter++
		delete(missingMap, urv)
	}
	//if r.debug {
	//	log.Println("runMultiGrabMap complete, deletes have run", closeChan)
	//}
	if closeChan {
		return true
	}
	r.Sleep()
	return false
}
func (r *Runner) workMultiMap(missingMap map[URL]struct{}, outCount *OutCounter, grabTkRep *token.MultiToken, tmpChan chan<- URL) (grabSuccess []URL, closeChan bool) {
	grabSuccess = make([]URL, 0, len(missingMap))
	//if r.debug {
	//	fmt.Println("Enter workMultiMap")
	//	defer fmt.Println("Exit workMultiMap")
	//}
	for urv := range missingMap {
		tim := StartTimer((time.Minute * 40), urv.String())
		if r.cycle() { // test if we're closed & pause as needed
			if r.debug {
				fmt.Println("Work Closing")
			}
			close(tim)
			return grabSuccess, true
		}
		if r.getConditional(urv, outCount, grabTkRep, tmpChan) {
			// If we sucessfully grab this (get a token etc)
			// then delete it fro the store
			//One we haven't visited we need to run a grab on
			// This fetch the URL and look for what to do
			grabSuccess = append(grabSuccess, urv)
		}
		close(tim)
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
		if r.closed() {
			runAuto = false
		} else {
			if current > target {
				r.Pause()
				r.abortableSleep(10 * time.Second)
			} else {
				r.Resume()
				r.abortableSleep(1 * time.Second)
			}
		}
	}
}
func (r *Runner) getConditional(urs URL, outCount *OutCounter, crawlChan *token.MultiToken, tmpChan chan<- URL) bool {
	//fmt.Println("getConditional",urs)
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
