package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"time"

	"github.com/cbehopkins/grab/grab"
	"github.com/cheggaaa/pb"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func cleanup(filename string, chani grab.UrlChannel, read_count int) {
	// Read from the channel until the channel drains and we empty the queue
	if filename != "" {
		ffile, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			log.Fatal("Cannot create file", err)
		}
		defer ffile.Close()
		f_read := 0
		for f_url := range chani {
			fmt.Fprintf(ffile, "%s\n", string(f_url))
			if read_count > 0 {
				if f_read > read_count {
					return
				}
				f_read++
			}
		}
	}
}

func main() {
	seedUrls := os.Args[1:]
	var out_count grab.OutCounter
	var show_progress_bar bool
	show_progress_bar = true
	load_seeds := true
	drip_feed := false

	// Channels
	chUrls := *grab.NewUrlChannel() // URLS to crawl
	chUrlsi := *grab.NewUrlChannel()
	chan_fetch_pushi := *grab.NewUrlChannel()

	fetch_url_store := grab.NewUrlStore()
	chan_fetch_push := fetch_url_store.PushChannel
	chan_fetch_pop := fetch_url_store.PopChannel

	// We've two token channels
	// This is our method of controlling the amount of traffic we generate
	// We generate a token at intervals and the fetchers do not
	// touch the network until they have one of these tokens.
	// If they do not need the token, they return it
	// Specify how often we generate a new token, how many to hold in the queue and a name
	var drip_crawl_interval int
	var drip_fetch_interval int
	if drip_feed {
		drip_crawl_interval = 1000
		drip_fetch_interval = 1000
	}
	crawl_token_chan := *grab.NewTokenChan(drip_crawl_interval, 2, "crawl")  // Number of URL crawlers
	fetch_token_chan := *grab.NewTokenChan(drip_fetch_interval, 20, "fetch") // Number of jpgs being fetched

	// This is actually the Crawler that takes URLS and spits out
	// jpg files to fetch
	// And feeds itself new URLs on the chUrls
	urlx := grab.NewUrlReceiver(chUrls, chan_fetch_push, &out_count, &crawl_token_chan)
	urlx.DbgFile("out_urls.txt")
	urlx.SetDbgUrls(!show_progress_bar)
	//urlx.SetAllInteresting(true)
	urlx.Start()

	// Kick off the seed process (concurrently)
	// Needs to be a gofunc as otherwise we get stuck here on channel send
	// As the receiver isn't started yet
	go func() {
		for _, urlt := range seedUrls {
			fmt.Println("Seeding :", urlt)
			out_count.Add()

			ai, err := url.Parse(string(urlt))
			check(err)
			domain_i := ai.Host
			// add it to the list of domains we can visit
			_ = urlx.VisitedA(domain_i)

			chUrls <- grab.Url(urlt)

			//fmt.Println("Send succeeded")
		}
	}()

	// Next, if they exist get the input fromt he seed files
	if load_seeds {
		out_count.Add()

		go grab.LoadFile("in_fetch.txt", chan_fetch_pushi, &out_count)
		go func() {
			for itm := range chan_fetch_pushi {
				ai, err := url.Parse(string(itm))
				check(err)
				domain_i := ai.Host
				_ = urlx.VisitedA(domain_i)
				chan_fetch_push <- itm
			}

		}()
	}
	if load_seeds {
		out_count.Add()

		go grab.LoadFile("in_urls.txt", chUrlsi, &out_count)
		go func() {
			for itm := range chUrlsi {
				ai, err := url.Parse(string(itm))
				check(err)
				domain_i := ai.Host
				_ = urlx.VisitedA(domain_i)
				chUrls <- itm
			}

		}()
	}

	fetch_inst := grab.NewFetcher(chan_fetch_pop, &out_count, &fetch_token_chan)
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
				time.Sleep(10000 * time.Millisecond)
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

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		ctrlc_attempts := 0
		for _ = range signalChan {
			fmt.Printf("\nReceived an interrupt, stopping services...\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
			if ctrlc_attempts == 0 {
				num_to_drain := urlx.UrlStore.Count()/2
				 fmt.Printf("\nDraining%d\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n",num_to_drain)
				go cleanup("unproc_url.txt", urlx.UrlStore.PopChannel, num_to_drain)
				ctrlc_attempts++
			} else if ctrlc_attempts == 1 {
				go cleanup("unproc_url.txt", urlx.UrlStore.PopChannel, 0)
				ctrlc_attempts++
			} else {
				os.Exit(1)
			}
		}
	}()

	fmt.Println("Waitng for out_count")
	out_count.Wait()
	close(chUrls)

}
