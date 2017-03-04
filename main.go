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
	drip_feed := false

	// Channels
	chUrls := *grab.NewUrlChannel() // URLS to crawl
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
	// Specify how often we generate a new token, how many to hold in the queue and a name
	var drip_crawl_interval int
	var drip_fetch_interval int
	if drip_feed {
		drip_crawl_interval = 10000
		drip_fetch_interval = 400
	}
	crawl_token_chan := *grab.NewTokenChan(drip_crawl_interval, 16, "") // Number of URL crawlers
	fetch_token_chan := *grab.NewTokenChan(drip_fetch_interval, 64, "") // Number of jpgs being fetched

	// This is actually the Crawler that takes URLS and spits out
	// jpg files to fetch
	// And feeds itself new URLs on the chUrls
	urlx := grab.NewUrlReceiver(chUrls, chan_fetch_push, &out_count, crawl_token_chan)
	urlx.DbgFile("out_urls.txt")
	urlx.SetDbgUrls(!show_progress_bar)
	//urlx.SetAllInteresting(true)
	urlx.Start()
	fetch_inst := grab.NewFetcher(chan_fetch_pop, &out_count, fetch_token_chan)
	fetch_inst.DbgFile("out_fetch.txt")
	fetch_inst.SetDbgUrls(!show_progress_bar)
	fetch_inst.SetTestJpg(false)
	fetch_inst.SetRunDownload(true)
	fetch_inst.Start()

	if show_progress_bar {
		// Show a progress bar

		// First keep track of max of things
		max_procs_seen := runtime.NumGoroutine()
		max_cout_count := out_count.Count

		// Create instances of the progress bars
		fet_bar := pb.New(fetch_url_store.InputCount())
		url_bar := pb.New(urlx.UrlStore.InputCount())
		gor_bar := pb.New(max_procs_seen)
		gor_bar.ShowTimeLeft = false
		oct_bar := pb.New(out_count.Count)

		// Name them
		fet_bar.Prefix("IMG Fetch :")
		url_bar.Prefix("TBD URLs  :")
		gor_bar.Prefix("Go Routine:")
		oct_bar.Prefix("Out Count :")
		// and start them
		pool, err := pb.StartPool(gor_bar, fet_bar, url_bar, oct_bar)
		if err != nil {
			panic(err)
		}
		// Start a routine that will occasionally update them
		go func() {
			for {
				time.Sleep(100 * time.Millisecond)
				current_go_procs := runtime.NumGoroutine()
				if max_procs_seen < current_go_procs {
					max_procs_seen = current_go_procs
					gor_bar.Total = int64(current_go_procs)
				}
				gor_bar.Set(current_go_procs)
				if out_count.Count > max_cout_count {
					max_cout_count = out_count.Count
					oct_bar.Total = int64(max_cout_count)
				}
				oct_bar.Set(out_count.Count)
				// TBD If equal then zero both
				fet_bar.Total = int64(fetch_url_store.InputCount())
				fet_bar.Set(fetch_url_store.OutputCount())
				url_bar.Total = int64(urlx.UrlStore.InputCount())
				url_bar.Set(urlx.UrlStore.OutputCount())
			}
		}()
		defer pool.Stop()

	}
	fmt.Println("Waitng for out_count")
	out_count.Wait()
	close(chUrls)

}
