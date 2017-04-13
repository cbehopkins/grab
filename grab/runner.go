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
}

func NewRunner(hm *Hamster,
	unvisit_urls, visited_urls *UrlMap,
) *Runner {
	itm := new(Runner)
	itm.hm = hm
	itm.unvisit_urls = unvisit_urls
	itm.visited_urls = visited_urls
	itm.recycle_time = 100 * time.Millisecond
	return itm
}
func (r *Runner) Wait() {
	r.wg.Wait()
}
func (r *Runner) Close() {
	close(r.grab_closer)
}
func (r *Runner) Shutdown() {
	r.Close()
	r.Wait() // Once we've closed it make sure it is closed
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
	grab_tk_rep := NewTokenChan(0, num_p_fetch, "grab")
	for {
		if r.unvisit_urls.Size() > 0 {
			tmp_chan := make(chan Url)
			out_count := NewOutCounter()
			out_count.Add()
			out_count.Dec()
			r.hm.SetGrabCh(tmp_chan)
			// Create the worker to sort any new Urls into the  two bins
			wgt := RunChan(tmp_chan, r.visited_urls, r.unvisit_urls, "")
			//fmt.Println("Lets see who to visit")
			// Create a channel of URLs that are missing from grab_tk_rep
			missing_chan := r.unvisit_urls.VisitMissing(grab_tk_rep)
			var is_closed bool
			for !is_closed {
				select {
				case <-r.grab_closer:
					is_closed = true
					fmt.Println("Grab Closer detected, shutting down grab process")
					out_count.Wait()
					close(tmp_chan)
					fmt.Println("Waiting for runChan to finish adding")
					wgt.Wait()
					fmt.Println("Grab closer complete")
					return

				case urv, ok := <-missing_chan:
					if !ok {
						is_closed = true
					} else {
						//fmt.Println("Maybe:", urv)
						out_count.Add()
						if r.visited_urls.Exist(urv) {
							// If we've already visited it then nothing to do
							r.unvisit_urls.Delete(urv)
							out_count.Dec()
							fmt.Println("Deleted:", urv)
						} else {
							// grab the Url urv
							// Send any new urls onto tmp_chan
							// and anything we should fetch goes on fetch_chan
							if r.hm.GrabIt(urv, out_count, grab_tk_rep) {
								// If we sucessfully grab this (get a token etc)
								// then delete it fro the store
								//One we haven't visited we need to run a grab on
								// This fetch the URL and look for what to do
								r.visited_urls.Set(urv)
								r.unvisit_urls.Delete(urv)
								//fmt.Println("Grab Started",urv)
							} else {
								//fmt.Println("Grab Aborted",urv)
							}
						}
						//fmt.Println("MaybeD:", urv)
					}
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

		} else {
			return
		}
		time.Sleep(r.recycle_time)
	}
}
