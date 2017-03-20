package main

import (
	"fmt"
	"sync"

	"github.com/cbehopkins/grab/grab"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func runChan(input_chan <-chan grab.Url, visited_urls, unvisit_urls *grab.UrlMap, wg *sync.WaitGroup) {
	for urv := range input_chan {
		//fmt.Println("runChan RX:", urv)
		if visited_urls.Exist(urv) {
			// If we've already visited it then nothing to do
		} else {
			// If we haven't visited it, then add it to the list of places to visit
			unvisit_urls.Set(urv)
		}
	}
	//fmt.Println("runChan Done")
	wg.Done()
}
func Grab(urs grab.Url, new_url_chan chan grab.Url, fetch_chan chan grab.Url,
	dv grab.DomVisitI, crawl_chan *grab.TokenChan) bool {

	token_got := grab.GetBase(string(urs))

	if crawl_chan.TryGetToken(token_got) {

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
		return true
	}
	return false
}

func Fetch(urf grab.Url, token_got string, fetch_tc *grab.TokenChan) {
	grab.FetchW(urf, true)
	fetch_tc.PutToken(token_got)
}
func main() {
	var wg sync.WaitGroup

	visited_urls := grab.NewUrlMap()
	unvisit_urls := grab.NewUrlMap()

	// urls read from a file and command line
	src_url_chan := make(chan grab.Url)

	// Read from anything that adds to the new_url_chan channel
	wg.Add(1)
	go runChan(src_url_chan, visited_urls, unvisit_urls, &wg)
	chUrlsi := *grab.NewUrlChannel()

	go grab.LoadFile("in_urls.txt", chUrlsi, nil)
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

	// Ensure they have written unto the queues first
	wg.Wait()
	fmt.Println("Run Seeded")

	// A fetch channel that goes away and writes intersting things to disk
	fetch_url_store := grab.NewUrlStore()
	chan_fetch_push := fetch_url_store.PushChannel
	chan_fetch_pop := fetch_url_store.PopChannel

	var fetch_wg sync.WaitGroup
	fetch_wg.Add(1)
	go func() {
		fetch_tk_rep := grab.NewTokenChan(0, 10, "fetch")
		for v := range chan_fetch_pop {
			token_get := grab.GetBase(string(v))
			fmt.Println("Fetch:", string(v))
			if fetch_tk_rep.TryGetToken(token_get) {
				// Get it as we have the token
				Fetch(v, token_get, fetch_tk_rep)
			} else {
				// Return the URL to the waiting list
				chan_fetch_push <- v
			}
		}
		fetch_wg.Done()
	}()
	wg.Add(1)
	// So here we want to pop things off the unvisited map
	go func() {
		grab_tk_rep := grab.NewTokenChan(0, 10, "grab")

		for {
			unvisited_urls := unvisit_urls.Size()
			fmt.Println("Looping Grab engine", unvisited_urls)
			if unvisited_urls > 0 {
				var wgt sync.WaitGroup
				tmp_chan := make(chan grab.Url)
				wgt.Add(1)
				// Create the worker to sort any new Urls into the  two bins
				go runChan(tmp_chan, visited_urls, unvisit_urls, &wgt)
				fmt.Println("Lets see who to visit")
				for urv := range unvisit_urls.Visit() {
					fmt.Println("Grab:", urv)
					if visited_urls.Exist(urv) {
						// If we've already visited it then nothing to do
						unvisit_urls.Delete(urv)
					} else {

						// grab the Url urv
						// Send any new urls onto tmp_chan
						// and anything we should fetch goes on fetch_chan
						if Grab(urv, tmp_chan, chan_fetch_push,
							dmv, grab_tk_rep) {
							// If we sucessfully grab this (get a token etc)
							// then delete it fro the store
							//One we haven't visited we need to run a grab on
							// This fetch the URL and look for what to do
							visited_urls.Set(urv)
							unvisit_urls.Delete(urv)
						}
					}
				}
				close(tmp_chan)
				// Wait for runChan to finish adding thing from the tmp chan to the
				// appropriate maps
				wgt.Wait()

			} else {
				wg.Done()
				return
			}
		}
	}()
	wg.Wait()
	close(chan_fetch_push)
	fetch_wg.Wait()
}
