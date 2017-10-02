package grab

import (
	"fmt"
	"sync"
	"time"
)

type Runner struct {
	wg           sync.WaitGroup
	hm           *Hamster
	recycle_time time.Duration
	unvisit_urls *UrlMap
	visited_urls *UrlMap
	grab_closer  chan struct{}
	counter      int
	st           time.Time
	grab_slowly  bool
	pause_lk     sync.Mutex
	pause        bool
}

func NewRunner(hm *Hamster,
	unvisit_urls, visited_urls *UrlMap,
) *Runner {
	itm := new(Runner)
	itm.hm = hm
	itm.unvisit_urls = unvisit_urls
	itm.visited_urls = visited_urls
	itm.recycle_time = 1 * time.Millisecond
	itm.st = time.Now()
	return itm
}
func (r *Runner) Wait() {
	//log.Println("Waiting for Runner")
	r.wg.Wait()
	//log.Println("Done Waiting for Runner")
}
func (r *Runner) Close() {
	if r.grab_closer != nil {
		close(r.grab_closer)
	}
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
	// We may have paused the runner as part of our throughput limits
	r.Resume()
	r.pause_lk.Lock() // Prevent us pausing again until shutdown
	defer r.pause_lk.Unlock()
	r.Close()
	r.Wait() // Once we've closed it make sure it is closed
	r.PrintThroughput()
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
	defer r.wg.Done()
	defer r.hm.Close()
	grab_tk_rep := NewTokenChan(num_p_fetch, "grab")
	var chan_closed bool
	grab_slowly := r.grab_slowly

	for !chan_closed {
		if r.unvisit_urls.Size() <= 0 {
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
			//out_count.Add()
			//out_count.Dec()

			// Create a channel that the hamster can add new urls to
			tmp_chan := make(chan Url)
			r.hm.SetGrabCh(tmp_chan)

			// Create the worker to sort any new Urls into the  two bins
			wgt := RunChan(tmp_chan, r.visited_urls, r.unvisit_urls, "")
			//fmt.Println("Lets see who to visit")

			// Create a map of URLs that are missing from grab_tk_rep
			//fmt.Println("Looking for something to do")
			missing_map_string := r.unvisit_urls.VisitMissing(grab_tk_rep)

			// Convert into a map of urls rather than string
			missing_map := make(map[Url]struct{})
			for urv := range missing_map_string {
				if r.visited_urls.ExistS(urv) {
					r.unvisit_urls.DeleteS(urv)
				} else {
					new_url := NewUrl(urv)
					_ = new_url.Base()
					missing_map[new_url] = struct{}{}
				}
			}
			//fmt.Printf("Runnin iter loop with %v\n",len(missing_map))
			for iter_cnt := 0; !chan_closed && (iter_cnt < 100) && (len(missing_map) > 0); iter_cnt++ {

				grab_success := make([]Url, 0, len(missing_map))
			map_itter:
				for urv := range missing_map {
					select {
					case _, ok := <-r.grab_closer:
						if !ok {
							chan_closed = true
							break map_itter
						}
					default:

						if r.getConditional(urv, out_count, grab_tk_rep) {
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

			//fmt.Println("Waiting for Grabs to finish after Visit")
			out_count.Wait()
			//fmt.Println("Waiting for runChan to finish adding")
			close(tmp_chan)
			// Wait for runChan to finish adding thing from the tmp chan to the
			// appropriate maps
			wgt.Wait()
			//fmt.Println("runChan has finished")

		}
		time.Sleep(r.recycle_time)
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
func (r *Runner) getConditional(urs Url, out_count *OutCounter, crawl_chan *TokenChan) bool {
	if r.hm.grabItWork(urs, out_count, crawl_chan) {
		r.visited_urls.Set(urs)
		if r.unvisit_urls != nil {
			r.unvisit_urls.Delete(urs)
		}
		return true
	}
	return false
}
func (r *Runner) getRegardless(itm Url, grab_tk_rep *TokenChan) {
	urv := itm.Url()
	if r.visited_urls.ExistS(urv) {
    if true {
      urv := itm.Url()
      fmt.Println("Skipping:", urv)
    }
		return
	} else {
		r.visited_urls.Set(itm)
		if r.unvisit_urls != nil {
			r.unvisit_urls.Delete(itm)
		}
    if true {
      urv := itm.Url()
      fmt.Println("Get:", urv)
    }
		r.hm.GrabT(
			itm, // The URL we are tasked with crawling
			"",  // Using universal token
			grab_tk_rep,
		)

	}
}
func (r *Runner) ChanGettery(start_url_chan chan Url, grab_tk_rep *TokenChan) {
  fmt.Println("Getter starting")
	for itm := range start_url_chan {
    if true {
      urv := itm.Url()
      fmt.Println("Get:", urv)
    }
		r.getRegardless(itm, grab_tk_rep)
	}
}
