package main

import (
	"fmt"
	//"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/cbehopkins/grab/grab"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func runChan(input_chan <-chan grab.Url, visited_urls, unvisit_urls *grab.UrlMap, wg *sync.WaitGroup, dbg_name string) {
	for urv := range input_chan {
		if dbg_name != "" {
			fmt.Printf("runChan, %s RX:%v\n", dbg_name, urv)
		}
		if visited_urls.Exist(urv) {
			// If we've already visited it then nothing to do
			//fmt.Printf("We've already visited %s\n", urv)
		} else {
			// If we haven't visited it, then add it to the list of places to visit
			unvisit_urls.Set(urv)
		}
		if dbg_name != "" {
			fmt.Printf("runChan, %s, has been set\n", dbg_name)
		}
	}
	//fmt.Println("runChan Done")
	wg.Done()
}
func Grab(urs grab.Url, hm *grab.Hamster, out_count *grab.OutCounter, crawl_chan *grab.TokenChan) bool {

	token_got := urs.Base()

	if crawl_chan.TryGetToken(token_got) {
		fmt.Println("Grab:", urs)
		if !hm.Shallow() {
			go func() {
				// Before we can send this, we need to take a copy of hm
				// so that we can parrallelise this
				hmp := hm.Copy()
				hmp.GrabT(urs, // The URL we are tasked with crawling
					token_got,
					crawl_chan,
				)
				//fmt.Println("Grabbed:", urs)
				// we don't want GrabT doing this as in
				// None Nil mode it will also add, which
				//we don't want for this design
				out_count.Dec()
			}()
			return true
		} else {
			hm.GrabT(urs, // The URL we are tasked with crawling
				token_got,
				crawl_chan,
			)
			out_count.Dec()
		}
	} else {
		out_count.Dec()
	}

	return false
}

func main() {
	var shutdown_in_progress sync.Mutex
	var wg sync.WaitGroup
	multiple_fetchers := true
	promiscuous := false
	shallow := !promiscuous
	all_interesting := false
	debug := false
	num_p_fetch := 4

	visited_fname := "/tmp/visited.gkvlite"
	unvisit_fname := "/tmp/unvisit.gkvlite"
	url_fn := "in_urls.txt"
	fetch_fn := "in_fetch.txt"
	bad_url_fn := "bad_urls.txt"

	// A fetch channel that goes away and writes intersting things to disk
	multi_fetch := grab.NewMultiFetch(multiple_fetchers)
	chan_fetch_push := multi_fetch.InChan

	grab_closer := make(chan struct{})
	var fetch_wg sync.WaitGroup

	var visited_urls *grab.UrlMap
	var unvisit_urls *grab.UrlMap
	// Open the maps and do not overwrite any we find
	visited_urls = grab.NewUrlMap(visited_fname, false)
	unvisit_urls = grab.NewUrlMap(unvisit_fname, false)

	// urls read from a file and command line
	src_url_chan := make(chan grab.Url)
	dmv := grab.NewDomVisit()

	// Read from anything that adds to the new_url_chan channel
	wg.Add(1) // one for runChan
	go runChan(src_url_chan, visited_urls, unvisit_urls, &wg, "")
	chUrlsi := *grab.NewUrlChannel()
	chUrlsb := *grab.NewUrlChannel()
	hm := grab.NewHamster(
		promiscuous,
		shallow,
		all_interesting,
		debug, // Print Urls
	)
	hm.SetDv(dmv)
	hm.SetFetchCh(chan_fetch_push)

	fmt.Println("Seeding URLs")
	// Close the channel but don't add to the (unsupplied) counter
	go grab.LoadFile(url_fn, chUrlsi, nil, true, false)
	go grab.LoadFile(bad_url_fn, chUrlsb, nil, true, false)
	wg.Add(1) // one for badUrls
	go func() {
		for itm := range chUrlsb {
			dmv.AddBad(itm.Url())
		}
		wg.Done()
	}()

	go func() {
		var grab_tk_rep *grab.TokenChan
		if shallow {
			grab_tk_rep = grab.NewTokenChan(0, num_p_fetch, "shallow")
			hm.SetGrabCh(src_url_chan)
		}
		for itm := range chUrlsi {
			fmt.Println("SeedURL:", itm)

			domain_i := itm.Base()
			if domain_i != "" {
				_ = dmv.VisitedA(domain_i)
				if shallow {
					// For shallow - thoroughly crawl all the seeds
					// But once that's done, switch shallow off
					hm.GrabT(itm, // The URL we are tasked with crawling
						"",
						grab_tk_rep,
					)
				} else {
					src_url_chan <- itm
				}
			}
		}
		close(src_url_chan)

	}()

	// Ensure they have written unto the queues first
	wg.Wait()
	fmt.Println("Run Seeded")
	hm.ClearShallow()
	fetch_wg.Add(1)

	go func() {
		multi_fetch.SetFileName(fetch_fn)
		multi_fetch.Worker(dmv)
		fetch_wg.Done()

	}()
	var grab_wg sync.WaitGroup
	grab_wg.Add(1)
	// So here we want to pop things off the unvisited map

	go func() {
		grab_tk_rep := grab.NewTokenChan(0, num_p_fetch, "grab")

		for {
			unvisited_urls := unvisit_urls.Size()
			if unvisited_urls > 0 {
				tmp_chan := make(chan grab.Url)
				var wgt sync.WaitGroup
				wgt.Add(1)
				out_count := grab.NewOutCounter()
				out_count.Add()
				out_count.Dec()
				hm.SetGrabCh(tmp_chan)
				// Create the worker to sort any new Urls into the  two bins
				go runChan(tmp_chan, visited_urls, unvisit_urls, &wgt, "")
				//fmt.Println("Lets see who to visit")
				// Create a channel of URLs that are missing from grab_tk_rep
				missing_chan := unvisit_urls.VisitMissing(grab_tk_rep)
				var is_closed bool
				for !is_closed {
					select {
					case <-grab_closer:
						is_closed = true
						fmt.Println("Grab Closer detected, shutting down grab process")
						out_count.Wait()
						close(tmp_chan)
						fmt.Println("Waiting for runChan to finish adding")
						wgt.Wait()
						fmt.Println("Grab closer complete")
						grab_wg.Done()
						return

					case urv, ok := <-missing_chan:
						if !ok {
							is_closed = true
						} else {
							//fmt.Println("Maybe:", urv)
							out_count.Add()
							if visited_urls.Exist(urv) {
								// If we've already visited it then nothing to do
								unvisit_urls.Delete(urv)
								out_count.Dec()
								fmt.Println("Deleted:", urv)
							} else {
								// grab the Url urv
								// Send any new urls onto tmp_chan
								// and anything we should fetch goes on fetch_chan
								if Grab(urv, hm, out_count,
									grab_tk_rep) {
									// If we sucessfully grab this (get a token etc)
									// then delete it fro the store
									//One we haven't visited we need to run a grab on
									// This fetch the URL and look for what to do
									visited_urls.Set(urv)
									unvisit_urls.Delete(urv)
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
				grab_wg.Done()
				return
			}
			<-time.After(100 * time.Millisecond)
		}
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	var grab_closer_closed bool

	go func() {
		cc_cnt := 0
		for _ = range signalChan {
			cc_cnt++
			if cc_cnt == 1 {
				shutdown_in_progress.Lock()
				go func() {
					fmt.Println("Ctrl-C Detected, flush and close")
					//var closing_wg sync.WaitGroup
					//closing_wg.Add(1)
					//go func() {
					//	unvisit_urls.Sync()
					//	visited_urls.Sync()
					//	unvisit_urls.Flush()
					//	visited_urls.Flush()
					//	closing_wg.Done()
					//}()
					if !grab_closer_closed {
						close(grab_closer)
						grab_closer_closed = true
					}
					// This should happen very quickly as
					// it merely has to finish the current spawning of a go process
					grab_wg.Wait() // Once we've closed it make sure it is closed
					fmt.Println("Grab Closed")
					multi_fetch.Scram()
					fetch_wg.Wait()
					fmt.Println("Fetch Complete")
					//closing_wg.Wait()
					unvisit_urls.Close()
					visited_urls.Close()
					shutdown_in_progress.Unlock()

					fmt.Println("Reload Passed")
					fmt.Println("All Complete")

					os.Exit(0)
				}()
			} else {
				os.Exit(1)
			}
		}
	}()
	grab_wg.Wait()
	shutdown_in_progress.Lock()
	if !grab_closer_closed {
		close(grab_closer)
		grab_closer_closed = true
	}
	shutdown_in_progress.Unlock()
	grab_wg.Wait() // Once we've closed it make sure it is closed
	fmt.Println("Grab Closed")
	multi_fetch.Close()
	fetch_wg.Wait()
	fmt.Println("Fetch Complete")
	unvisit_urls.Close()
	visited_urls.Close()
}
