package main

import (
	"fmt"
	"github.com/cbehopkins/grab/grab"
	"os"
)


func main() {
	seedUrls := os.Args[1:]
	var out_count grab.OutCounter

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
	if true {
		go grab.LoadFile("in_fetch.txt", chan_fetch_push, &out_count)
	}
	if true {
		go grab.LoadFile("in_urls.txt", chUrls, &out_count)
	}

	// We've two token channels
	// This is our method of controlling the amount of traffic we generate
	// We generate a token at intervals and the fetchers do not
	// touch the network until they have one of these tokens.
	// If they do not need the token, they return it
	crawl_token_chan := *grab.NewTokenChan(20000, "")
	fetch_token_chan := *grab.NewTokenChan(4000, "")

	// This is actually the Crawler that takes URLS and spits out
	// jpg files to fetch
	// And feeds itself new URLs on the chUrls
	go grab.UrlReceiver(chUrls, chan_fetch_push, &out_count, crawl_token_chan, "out_urls.txt")
	go grab.FetchReceiver(       chan_fetch_pop, &out_count, fetch_token_chan, "out_fetch.txt")

	out_count.Wait()
	close(chUrls)
}
