package main

import (
	"fmt"
	"github.com/cbehopkins/grab/grab"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func runChan(input_chan <-chan grab.Url, visited_urls, unvisit_urls *grab.UrlMap, wg *grab.OutCounter, dbg_name string) {
	for urv := range input_chan {
		if dbg_name != "" {
			fmt.Printf("runChan, %s RX:%v\n", dbg_name, urv)
		}
		if visited_urls.Exist(urv) {
			// If we've already visited it then nothing to do
		} else {
			// If we haven't visited it, then add it to the list of places to visit
			unvisit_urls.Set(urv)
		}
		if dbg_name != "" {
			fmt.Printf("runChan, %s, has been set\n", dbg_name)
		}
	}
	//fmt.Println("runChan Done")
	wg.Dec()
}
func Grab(urs grab.Url, new_url_chan chan grab.Url, fetch_chan chan grab.Url,
	out_count *grab.OutCounter,
	dv grab.DomVisitI, crawl_chan *grab.TokenChan) bool {

	token_got := grab.GetBase(string(urs))

	if crawl_chan.TryGetToken(token_got) {
		fmt.Println("Grab:", urs)
		go func() {
			grab.GrabT(urs, // The URL we are tasked with crawling
				token_got,
				false, // all interesting
				true,  // print_urls
				nil,
				dv,
				new_url_chan,
				fetch_chan,
				crawl_chan,
			)
			out_count.Dec()
		}()
		return true
	}
	return false
}

func Fetch(urf grab.Url, token_got string, fetch_tc *grab.TokenChan, out_count *grab.OutCounter) {
	grab.FetchW(urf, false)
	fetch_tc.PutToken(token_got)
	out_count.Dec()
}
func main() {
	wg := grab.NewOutCounter()
	// A fetch channel that goes away and writes intersting things to disk
	fetch_url_store := grab.NewUrlStore()
	chan_fetch_push := fetch_url_store.PushChannel
	chan_fetch_pop := fetch_url_store.PopChannel
	can_closer := make(chan struct{})
	grab_closer := make(chan struct{})
	var fetch_wg sync.WaitGroup

	visited_urls := grab.NewUrlMap("/tmp/visited.gkvlite", false)
	unvisit_urls := grab.NewUrlMap("/tmp/unvisit.gkvlite", true)

	// urls read from a file and command line
	src_url_chan := make(chan grab.Url)

	// Read from anything that adds to the new_url_chan channel
	wg.Add()	// one for runChan, one for LoadFile
	wg.Add()
	go runChan(src_url_chan, visited_urls, unvisit_urls, wg, "")
	chUrlsi := *grab.NewUrlChannel()

	fmt.Println("Seeding URLs")
	go grab.LoadFile("in_urls.txt", chUrlsi, nil, true, false)
	dmv := grab.NewDomVisit()
	go func() {
		for itm := range chUrlsi {
			//fmt.Println("SeedURL:", itm)

			domain_i := grab.GetBase(string(itm))
			_ = dmv.VisitedA(domain_i)
			src_url_chan <- itm
		}
		close(src_url_chan)

	}()
	go func() {
		fmt.Println("Seeding Fetch")
		grab.LoadFile("in_fetch.txt", chan_fetch_push, nil,false,false)
		fmt.Println("Fetch Seeded")
		wg.Dec()
	}()

	// Ensure they have written unto the queues first
	wg.Wait()
	fmt.Println("Run Seeded")

	fetch_wg.Add(1)

	go func() {
		
		fetch_tk_rep := grab.NewTokenChan(0, 10, "fetch")
		is_closed := false
		out_count := grab.NewOutCounter()
		out_count.Add()	// Nonesense here is to initialise it properly
		out_count.Dec()
		for {
			select {
			case v, ok := <-chan_fetch_pop:
				if !ok {
					fmt.Println("Pop Closed, waiting for out_count")
					out_count.Wait()
					fmt.Println("out_count complete, makring us as done")
					fetch_wg.Done()
					return
				}
				token_get := grab.GetBase(string(v))
				if is_closed {
					fetch_tk_rep.GetToken(token_get)
					// If we are closed, we can't recycle
					// so wait until we can get the token
					fmt.Println("FetchClose:", string(v))
					out_count.Add()
					go Fetch(v, token_get, fetch_tk_rep, out_count)
				} else if fetch_tk_rep.TryGetToken(token_get) {
					fmt.Println("Fetch:", string(v))
					// Get it as we have the token
					out_count.Add()
					go Fetch(v, token_get, fetch_tk_rep, out_count)
				} else if !is_closed {
					// Return the URL to the waiting list
					chan_fetch_push <- v
				}
			case _, ok := <-can_closer:
				is_closed = true
				if ok {
					close(chan_fetch_push)
				}
			}
		}
	}()
	var grab_wg sync.WaitGroup
	grab_wg.Add(1)
	// So here we want to pop things off the unvisited map
	go func() {
		grab_tk_rep := grab.NewTokenChan(0, 10, "grab")

		for {
			unvisited_urls := unvisit_urls.Size()
			if unvisited_urls > 0 {
				tmp_chan := make(chan grab.Url)
				wgt := grab.NewOutCounter()
				wgt.Add()
				out_count := grab.NewOutCounter()
				// Create the worker to sort any new Urls into the  two bins
				go runChan(tmp_chan, visited_urls, unvisit_urls, wgt, "")
				fmt.Println("Lets see who to visit")
				//for urv := range unvisit_urls.VisitMissing(grab_tk_rep)
				missing_chan := unvisit_urls.VisitMissing(grab_tk_rep)
				var is_closed bool
				for !is_closed {
					select {
					case <-grab_closer:
						is_closed = true
					case urv, ok := <-missing_chan:
						if !ok {
							is_closed = true
						} else {
							out_count.Add()
							//fmt.Println("Maybe:", urv)
							if visited_urls.Exist(urv) {
								// If we've already visited it then nothing to do
								unvisit_urls.Delete(urv)
								out_count.Dec()
							} else {
								// grab the Url urv
								// Send any new urls onto tmp_chan
								// and anything we should fetch goes on fetch_chan
								if Grab(urv, tmp_chan, chan_fetch_push,
									out_count,
									dmv, grab_tk_rep) {
									// If we sucessfully grab this (get a token etc)
									// then delete it fro the store
									//One we haven't visited we need to run a grab on
									// This fetch the URL and look for what to do
									visited_urls.Set(urv)
									unvisit_urls.Delete(urv)
								} else {
									out_count.Dec()
								}
							}
						}
					}
				}
				out_count.Wait()
				close(tmp_chan)
				// Wait for runChan to finish adding thing from the tmp chan to the
				// appropriate maps
				wgt.Wait()

			} else {
				grab_wg.Done()
				return
			}
			<-time.After(100 * time.Millisecond)
		}
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func() {
		for _ = range signalChan {
			var ccwg sync.WaitGroup
			ccwg.Add(5)
			fmt.Println("Ctrl-C Detected, flush and close")
			go func() {
				unvisit_urls.Flush()
				unvisit_urls.Sync()
				ccwg.Done()
			}()
			go func() {
				visited_urls.Flush()
				visited_urls.Sync()
				ccwg.Done()
			}()
			go func() {
				can_closer <- struct{}{}
				fmt.Println("Push Closed")
				close(can_closer)
				ccwg.Done()
			}()
			go func() {
				grab_closer <- struct{}{}
				fmt.Println("Grab Closed")
				close(grab_closer)
				ccwg.Done()
			}()
			go func() {
				grab.SaveFile("in_fetch.txt", chan_fetch_pop, nil)
				fmt.Println("in_fetch saved")
				fetch_wg.Wait()
				fmt.Println("Fetch Complete")
				ccwg.Done()
			}()
			ccwg.Wait()
			ccwg.Add(2)
			go func() {
				unvisit_urls.Close()
				ccwg.Done()
			}()
			go func() {
				visited_urls.Close()
				ccwg.Done()
			}()
			ccwg.Wait()
			os.Exit(0)
		}
	}()
	grab_wg.Wait()
	fmt.Println("Closing Push Channel")
	can_closer <- struct{}{}
	fmt.Println("Push Closed")
	close(can_closer)
	fmt.Println("Waiting for Fetch to close")
	fetch_wg.Wait()

	unvisit_urls.Close()
	visited_urls.Close()
}
