package grab

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type Runner struct {
	wg sync.WaitGroup
	hm *Hamster

	recycle_time time.Duration
	ust          *uniStore
	grab_closer  chan struct{}
	counter      int
	st           time.Time
	grab_slowly  bool
	pause_lk     sync.Mutex
	pause        bool
	linear       bool
}

func NewRunner(
	chan_fetch_push chan<- Url,
	bad_url_fn, dir string,
	compact,
	promiscuous,
	all_interesting,
	debug, // Print Urls
	polite bool,
) *Runner {
	itm := new(Runner)
	// A DomVisit tracks what domains we're allowed to visit
	// Any domains we come across not in this list will not be visited
	itm.ust = newUniStore(dir, compact, bad_url_fn)

	// The Hamster is the thing that goes out and
	// Grabs stuff, looking for interesting things to fetch
	itm.hm = NewHamster(
		promiscuous,
		all_interesting,
		debug, // Print Urls
		polite,
	)
	itm.hm.SetDv(itm.ust.getDv())
	itm.hm.SetFetchCh(chan_fetch_push)
	itm.recycle_time = 1 * time.Millisecond
	itm.st = time.Now()
	return itm
}

func (r *Runner) SeedWg(url_fn string, promiscuous bool) *sync.WaitGroup {
	src_url_chan := r.hm.dv.Seed(url_fn, promiscuous)
	wgrc := r.RunChan(src_url_chan, "")
	r.ust.waitLoad()
	return wgrc
}
func (r *Runner) LockTest() {
	r.ust.lockTest()
}
func (r *Runner) FlushSync() *sync.WaitGroup {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		r.ust.flushSync()
		wg.Done()
		fmt.Println("Done Flush and Sync")
	}()
	return wg
}
func (r *Runner) VisitCount() int {
	return r.ust.visitCount()
}
func (r *Runner) UnvisitCount() int {
	return r.ust.unvisitCount()
}
func (r *Runner) RunChan(input_chan <-chan Url, dbg_name string) *sync.WaitGroup {
	return r.ust.runChan(input_chan, dbg_name)
}
func (r *Runner) Wait() {
	log.Println("Waiting for Runner")
	r.wg.Wait()
	log.Println("Done Waiting for Runner")
}
func (r *Runner) Close() {
	//if r.grab_closer != nil {
	close(r.grab_closer)
	//}
}
func (r *Runner) GoSlow() {
	r.grab_slowly = true
}
func (r *Runner) PrintThroughput() {
	elapsed := time.Since(r.st).Seconds()
	//fmt.Printf("%d items in %v seconds",r.counter,elapsed)
	tp := float64(r.counter) / elapsed
	tp_int := int64(tp)
	fmt.Printf("\n\n%v Items per Second\n", tp_int)
}
func (r *Runner) Resume() {
	r.pause_lk.Lock()
	if r.pause {
		fmt.Printf("\nResuming Grabber\n\n\n")
	}
	r.pause = false
	r.pause_lk.Unlock()
}
func (r *Runner) Pause() {
	r.pause_lk.Lock()
	if !r.pause {
		fmt.Printf("\nPausing Grabber\n\n\n")
	}
	r.pause = true
	r.pause_lk.Unlock()
}
func (r *Runner) Shutdown() {
	var closing_wg *sync.WaitGroup
	closing_wg = r.FlushSync()

	// We may have paused the runner as part of our throughput limits
	r.Resume()
	r.pause_lk.Lock() // Prevent us pausing again until shutdown
	defer r.pause_lk.Unlock()
	fmt.Println("Close Runneri")
	r.Close()
	fmt.Println("Close completer")
	r.Wait() // Once we've closed it make sure it is closeda
	fmt.Println("Waited for runner, closing_wg")
	closing_wg.Wait()
	fmt.Println("Close universal store")
	r.ust.closeS()

	fmt.Println("Store closed, printing throughput")
	r.PrintThroughput()
	r.ust.closeDv()
	fmt.Println("Closed Dv")
}
func (r *Runner) SetRTime(rt time.Duration) {
	r.recycle_time = rt
}
func (r *Runner) GrabRunner(num_p_fetch int) {
	r.wg.Add(1)
	r.grab_closer = make(chan struct{})
	go r.grabRunner(num_p_fetch)
}

func (r *Runner) grabRunner(num_p_fetch int) {
	fmt.Println("Testing Locks")
	r.ust.lockTest()
	fmt.Println("Lock test pass")
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
	grab_tk_rep := NewTokenChan(num_p_fetch, "grab")
	var chan_closed bool
	grab_slowly := r.grab_slowly
	if r.linear {
		out_count := NewOutCounter()
		out_count.initDc()
		tmp_chan := make(chan Url)
		// Create the worker to sort any new Urls into the  two bins
		wgt := r.ust.runChan(tmp_chan, "")

		somethingSkipped := true
		for somethingSkipped {
			var delList []Url
			somethingSkipped = false
			///url_chan := r.ust.VisitAll(r.grab_closer)
      url_chan := r.ust.Visit()
      chan_closed = false
			for !chan_closed {
				select {
				case urv, ok := <-url_chan:
					if !ok {
						chan_closed = true
						fmt.Println("url_chan closed")
						somethingSkipped = false
					} else {
						urv.Initialise()
						//fmt.Println("Crawling:", urv)
						if r.hm.grabItWork(urv, out_count, grab_tk_rep, tmp_chan) {
							delList = append(delList, urv)
            // while we are using Visit, not VisitAll
							somethingSkipped = true
              if false {
								fmt.Println("Crawled:", urv)
							}
						} else {
							if false {
								fmt.Println("Skipped:", urv)
							}
							somethingSkipped = true
						}
					}
				}
			}
			for _, urs := range delList {
				r.ust.setVisited(urs)
			}
			if somethingSkipped {
				if grab_slowly {
					time.Sleep(10 * time.Second)
				} else {
					// Give a Ping time for Grabbers to work
					// Yes it's not enough, but after a couple of itterations we should
					// get a nice balanced system
					time.Sleep(10 * time.Millisecond)
				}
			}
		}

		if UseParallelGrab {
			out_count.Wait()
		}
		fmt.Println("Waiting for runChan to finish adding")
		close(tmp_chan)

		wgt.Wait()
		time.Sleep(r.recycle_time)
	} else {
		for !chan_closed {
			if r.ust.unvisitSize() <= 0 {
				return
			} else {
				select {
				case _, ok := <-r.grab_closer:
					if !ok {
						chan_closed = true
						continue
					}
				default:
				}
				r.pause_lk.Lock()
				we_pause := r.pause
				r.pause_lk.Unlock()

				if we_pause {
					time.Sleep(10 * time.Second)
					continue
				}
				out_count := NewOutCounter()
				out_count.initDc()

				// Create a channel that the hamster can add new urls to
				tmp_chan := make(chan Url)
				//r.hm.SetGrabCh(tmp_chan)
				fmt.Println("Create runchan worker")
				// Create the worker to sort any new Urls into the  two bins
				wgt := r.ust.runChan(tmp_chan, "")
				fmt.Println("Asking for missing")
				// Create a map of URLs that are missing from grab_tk_rep
				missing_map_string := r.ust.getMissing(grab_tk_rep)
				r.ust.flushSync()
				fmt.Println("Missing string rxd")
				// Convert into a map of urls rather than string
				missing_map := make(map[Url]struct{})
				for urv := range missing_map_string {
					new_url := r.ust.retrieveUnvisted(urv)
					if new_url != nil {
						missing_map[*new_url] = struct{}{}
					}
				}
				fmt.Printf("Runnin iter loop with %v\n", len(missing_map))
				for iter_cnt := 0; !chan_closed && (iter_cnt < 100) && (len(missing_map) > 0); iter_cnt++ {

					grab_success := make([]Url, 0, len(missing_map))
				map_itter:
					for urv := range missing_map {
						//fmt.Println("Run Url", urv)
						urv.Base()
						select {
						case _, ok := <-r.grab_closer:
							if !ok {
								chan_closed = true
								break map_itter
							}
						default:

							if r.getConditional(urv, out_count, grab_tk_rep, tmp_chan) {
								// If we sucessfully grab this (get a token etc)
								// then delete it fro the store
								//One we haven't visited we need to run a grab on
								// This fetch the URL and look for what to do
								grab_success = append(grab_success, urv)
							}
						}

					}

					for _, urv := range grab_success {
						r.counter++
						delete(missing_map, urv)
					}
					if grab_slowly {
						time.Sleep(10 * time.Second)
					} else {
						// Give a Ping time for Grabbers to work
						// Yes it's not enough, but after a couple of itterations we should
						// get a nice balanced system
						time.Sleep(10 * time.Millisecond)
					}
				}

				fmt.Println("Waiting for Grabs to finish after Visit")
				if UseParallelGrab {
					out_count.Wait()
				}
				fmt.Println("Waiting for runChan to finish adding")
				close(tmp_chan)
				// Wait for runChan to finish adding thing from the tmp chan to the
				// appropriate maps
				wgt.Wait()
				fmt.Println("runChan has finished")

			}
			time.Sleep(r.recycle_time)
		}
	}
}
func (runr *Runner) AutoPace(multi_fetch *MultiFetch, target int) {
	// When activated pace the runner.
	// when there is more than target things to fetch. Pause the runner
	// then less, then activate it

	run_auto := true
	for current := multi_fetch.Count(); run_auto; current = multi_fetch.Count() {
		select {
		case _, ok := <-runr.grab_closer:
			if !ok {
				run_auto = false
			}
		default:
			if current > target {
				runr.Pause()
				time.Sleep(10 * time.Second)
			} else {
				runr.Resume()
				time.Sleep(1 * time.Second)
			}
		}
	}

}
func (r *Runner) getConditional(urs Url, out_count *OutCounter, crawl_chan *TokenChan, tmp_chan chan<- Url) bool {
	if r.hm.grabItWork(urs, out_count, crawl_chan, tmp_chan) {
		r.ust.setVisited(urs)
		return true
	}
	return false
}

func (r *Runner) Dump(unvisit_filename, visit_filename string) {
	if unvisit_filename != "" {
		r.ust.dumpUnvisited(unvisit_filename)
	}
	if visit_filename != "" {
		r.ust.dumpVisited(visit_filename)
	}
}
func (r *Runner) Seed(url_fn string, promiscuous bool) chan Url {
	return r.ust.Seed(url_fn, promiscuous)
}

func (r *Runner) ClearVisited() {
	r.ust.clearVisited()
}

func (r *Runner) GoodUrl(url_in Url) bool {
	return r.ust.GoodUrl(url_in)
}
func (r *Runner) VisitedQ(url_in string) bool {
	return r.ust.VisitedQ(url_in)
}

func (r *Runner) VisitedA(url_in string) bool {
	return r.ust.VisitedA(url_in)
}

func (r *Runner) SetLinear() {
	r.linear = true
}
func (r *Runner) ClearLinear() {
	r.linear = false
}
