package main

import (
	"bufio"
	"fmt"
	"github.com/cbehopkins/grab/grab"
	"log"
	"math/rand"
	"os"
	"time"
)

func Readln(r *bufio.Reader) (string, error) {
	var (
		isPrefix bool  = true
		err      error = nil
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}

func token_source(sleep int, token_chan chan struct{}, action string) {
	r := rand.New(rand.NewSource(1))
	for {
		token_chan <- *new(struct{})
		var time_to_sleep time.Duration
		if sleep != 0 {
			time_to_sleep = time.Millisecond * time.Duration(r.Intn(sleep))
		}
		if action != "" {
			fmt.Printf("It will be %s until next %s\n", time_to_sleep, action)
		}
		time.Sleep(time_to_sleep)
	}
}

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
	crawl_token_chan := make(chan struct{}, 16)
	go token_source(20000, crawl_token_chan, "")
	// This is actually the Crawler that takes URLS and spits out
	// jpg files to fetch
	// And feeds itself new URLs on the chUrls
	go grab.UrlReceiver(chUrls, chan_fetch_push, &out_count, crawl_token_chan)

	// This is now the fetch worker
	// Fetch up to n simultaneous items
	fetch_token_chan := make(chan struct{}, 16)
	//for i := 0; i < 4; i++ {
	//	fetch_token_chan <- *new(struct{})
	//}
	go token_source(4000, fetch_token_chan, "")

	file, err := os.Create("out_items.txt")
	if err != nil {
		log.Fatal("Cannot create file", err)
	}
	defer file.Close()
	if true {
		go func() {
			f, err := os.Open("in_items.txt")
			if err != nil {
				fmt.Printf("error opening file: %v\n", err)
				os.Exit(1)
				return
			}
			r := bufio.NewReader(f)
			s, e := Readln(r)
			for e == nil {
				fmt.Println("Fast adding ", s)
				chan_fetch_push <- grab.Url(s)
				s, e = Readln(r)
			}
		}()
	}
	go func() {
		fetched_urls := make(map[grab.Url]bool)
		for fetch_url := range chan_fetch_pop {
			_, ok := fetched_urls[fetch_url]
			//if false {
			if !ok {
				fetched_urls[fetch_url] = true
				//fmt.Println("Fetching token")
				<-fetch_token_chan
				//fmt.Println("Got Token")
				go func() {
					//used_network := false
					//grab.Fetch(fetch_url)
					fmt.Println("Fetching :", fetch_url)
					fmt.Fprintf(file, "%s\n", string(fetch_url))
					//fetch_token_chan <- true
					//if !used_network {
					fmt.Println("Not used fetch token, returning")
					//	fetch_token_chan <- *new(struct{})
					//}
				}()
			}
		}
	}()

	out_count.Wait()
	close(chUrls)
}
