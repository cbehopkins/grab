package grab

import (
	"fmt"
	"runtime"
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
	close(r.grab_closer)
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
func (r *Runner) Shutdown() {
	r.Close()
	r.Wait() // Once we've closed it make sure it is closed
	r.hm.Close()
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
	grab_tk_rep := NewTokenChan(num_p_fetch, "grab")
	var chan_closed bool
	grab_slowly := r.grab_slowly

	for !chan_closed {
		if r.unvisit_urls.Size() <= 0 {
			return
		} else {
			out_count := NewOutCounter()
			out_count.Add()
			out_count.Dec()

			// Create a channel that the hamster can add new urls to
			tmp_chan := make(chan Url)
			r.hm.SetGrabCh(tmp_chan)

			// Create the worker to sort any new Urls into the  two bins
			wgt := RunChan(tmp_chan, r.visited_urls, r.unvisit_urls, "")
			//fmt.Println("Lets see who to visit")

			// Create a map of URLs that are missing from grab_tk_rep
			//fmt.Println("Looking for something to do")
			missing_map := r.unvisit_urls.VisitMissing(grab_tk_rep)
			//fmt.Println("Got something to do", len(missing_map))
			grab_already := make([]Url, 0, len(missing_map))
			for urv, _ := range missing_map {
				if r.visited_urls.Exist(urv) {
					// If we've already visited it then nothing to do
					r.unvisit_urls.Delete(urv)
					//fmt.Println("Deleted:", urv)
					grab_already = append(grab_already, urv)
				}
			}
			for _, urv := range grab_already {
				delete(missing_map, urv)
			}
			select {
			case _, ok := <-r.grab_closer:
				if !ok {
					chan_closed = true
				}
			default:
			}
			//fmt.Printf("Runnin iter loop with %v\n",len(missing_map))
			for iter_cnt := 0; !chan_closed && (iter_cnt < 100) && (len(missing_map) > 0); iter_cnt++ {
				grab_success := make([]Url, 0, len(missing_map))
			map_itter:
				for urv, _ := range missing_map {
					select {
					case _, ok := <-r.grab_closer:
						if !ok {
							chan_closed = true
							break map_itter
						}
					default:
					}
					// Whatever happens GrabIt will do the Dec()
					out_count.Add()

					if r.hm.GrabIt(urv, out_count, grab_tk_rep) {
						// If we sucessfully grab this (get a token etc)
						// then delete it fro the store
						//One we haven't visited we need to run a grab on
						// This fetch the URL and look for what to do
						r.visited_urls.Set(urv)
						r.unvisit_urls.Delete(urv)
						grab_success = append(grab_success, urv)
						//fmt.Println("Grab Started",urv)
					} else {
						//fmt.Println("Grab Aborted",urv)
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

			// Experiment to reduce memory consumption
			if false {
				r.unvisit_urls.Flush()
				r.visited_urls.Flush()
				runtime.GC()
			}
		}
		time.Sleep(r.recycle_time)
	}
}
