package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/cbehopkins/grab/grab"
	"github.com/cheggaaa/pb"
)

func main() {
	seedUrls := os.Args[1:]
	var out_count grab.OutCounter
	var show_progress_bar bool
	show_progress_bar = true
	load_seeds := true

	// Channels
	chUrls := *grab.NewUrlChannel() // URLS to crawl
	//chan_fetch := *grab.NewUrlChannel() // Files to fetch
	fetch_url_store := grab.NewUrlStore()
	chan_fetch_push := fetch_url_store.PushChannel
	chan_fetch_pop := fetch_url_store.PopChannel

	// Kick off the seed process (concurrently)
	// Needs to be a gofunc as otherwise we get stuck here on channel send
	// As the receiver isn't started yet
	go func() {
		for _, url := range seedUrls {
			fmt.Println("Seeding :", url)
			out_count.Add()
			chUrls <- grab.Url(url)
			//fmt.Println("Send succeeded")
		}
	}()

	// Next, if they exist get the input fromt he seed files
	if load_seeds {
		out_count.Add()

		go grab.LoadFile("in_fetch.txt", chan_fetch_push, &out_count)
	}
	if load_seeds {
		out_count.Add()

		go grab.LoadFile("in_urls.txt", chUrls, &out_count)
	}

	// We've two token channels
	// This is our method of controlling the amount of traffic we generate
	// We generate a token at intervals and the fetchers do not
	// touch the network until they have one of these tokens.
	// If they do not need the token, they return it
	crawl_token_chan := *grab.NewTokenChan(10000, 16, "")
	fetch_token_chan := *grab.NewTokenChan(400, 16, "")

	// This is actually the Crawler that takes URLS and spits out
	// jpg files to fetch
	// And feeds itself new URLs on the chUrls
	urlx := grab.NewUrlReceiver(chUrls, chan_fetch_push, &out_count, crawl_token_chan)
	urlx.DbgFile("out_urls.txt")
	urlx.DbgUrls(!show_progress_bar)
	//urlx.AllInteresting(true)
	urlx.Start()
	fetch_inst := grab.NewFetcher(chan_fetch_pop, &out_count, fetch_token_chan, 8)
	fetch_inst.DbgFile("out_fetch.txt")
	fetch_inst.DbgUrls(!show_progress_bar)
	fetch_inst.SetTestJpg(false)
	fetch_inst.SetRunDownload(true)
	fetch_inst.Start()

	if show_progress_bar {
		// Show a progress bar
		fetch_bar := pb.New(fetch_url_store.InputCount())
		url_bar := pb.New(urlx.UrlStore.InputCount())
		max_procs_seen := runtime.NumGoroutine()
		gor_bar := pb.New(max_procs_seen)
		gor_bar.ShowTimeLeft = false
		// and start
		pool, err := pb.StartPool(fetch_bar, url_bar, gor_bar)
		if err != nil {
			panic(err)
		}

		go func() {
			for {
				time.Sleep(1000 * time.Millisecond)
				current_go_procs := runtime.NumGoroutine()
				if max_procs_seen < current_go_procs {
					max_procs_seen = current_go_procs
					gor_bar.Total = int64(current_go_procs)
				}
				gor_bar.Set(current_go_procs)
				// TBD If ewusl then zero both
				fetch_bar.Total = int64(fetch_url_store.InputCount())
				fetch_bar.Set(fetch_url_store.OutputCount())
				url_bar.Total = int64(urlx.UrlStore.InputCount())
				url_bar.Set(urlx.UrlStore.OutputCount())
			}
		}()
		defer pool.Stop()

	}
	out_count.Wait()
	close(chUrls)

}
