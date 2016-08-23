package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// Helper function to pull the href attribute from a Token
func getHref(t html.Token) (ok bool, href string) {
	// Iterate over all of the Token's attributes until we find an "href"
	for _, a := range t.Attr {
		if a.Key == "href" {
			href = a.Val
			ok = true
		}
	}

	// "bare" return will return the variables (ok, href) as defined in
	// the function definition
	return
}

// Extract all http** links from a given webpage
func crawl(url Url, ch UrlChannel, fetch_chan UrlChannel, out_count *OutCounter) {
	resp, err := http.Get(string(url))
	defer out_count.Dec()

	if err != nil {

		fmt.Println("ERROR: Failed to crawl \"" + url + "\"")
		switch err {
		case http.ErrBodyReadAfterClose:
			fmt.Println("Read after close error")
		case http.ErrMissingFile:
			fmt.Println("Missing File")
		case http.ErrHeaderTooLong:
			fmt.Println("ErrHeaderTooLong")
		case http.ErrShortBody:
			fmt.Println("ErrShortBody")
		case http.ErrNotSupported:
			fmt.Println("ErrNotSupported")
		case http.ErrUnexpectedTrailer:
			fmt.Println("ErrUnexpectedTrailer")
		case http.ErrMissingContentLength:
			fmt.Println("ErrMissingContentLength")
		case http.ErrNotMultipart:
			fmt.Println("ErrNotMultipart")
		case http.ErrMissingBoundary:
			fmt.Println("ErrMissingBoundary")
		default:
			panic(err)
		}
		panic("See above")
		return
	}

	b := resp.Body
	defer b.Close()
	defer io.Copy(ioutil.Discard, b)
	z := html.NewTokenizer(b)

	for {
		tt := z.Next()

		switch {
		case tt == html.ErrorToken:
			// End of the document, we're done
			return
		case tt == html.StartTagToken:
			t := z.Token()

			// Check if the token is an <a> tag
			isAnchor := t.Data == "a"
			if !isAnchor {
				continue
			}

			// Extract the href value, if there is one
			ok, url := getHref(t)
			if !ok {
				continue
			}

			// Make sure the url begines in http**
			hasProto := strings.Index(url, "http") == 0
			if hasProto {

				is_jpg := strings.Contains(url, ".jpg")
				if is_jpg {
					fetch_chan <- Url(url)
				} else {
					out_count.Add()
					ch <- Url(url)
					//fmt.Println("Interesting url", url
				}
			}
		}
	}
}
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}
func fetch(fetch_url Url) {
	array := strings.Split(string(fetch_url), "/")
	var fn string
	if len(array) > 0 {
		fn = array[len(array)-1]
	}

	dir_struct := array[2 : len(array)-1]
	dir_str := strings.Join(dir_struct, "/")
	potential_file_name := dir_str + "/" + fn
	if _, err := os.Stat(potential_file_name); os.IsNotExist(err) {
		fmt.Printf("Fetching %s, fn:%s\n", fetch_url, fn)
		os.MkdirAll(dir_str, os.ModeDir)

		out, err := os.Create(potential_file_name)
		defer out.Close()
		check(err)
		resp, err := http.Get(string(fetch_url))
		defer resp.Body.Close()
		check(err)
		_, err = io.Copy(out, resp.Body)
		check(err)
	} else {
		fmt.Println("skipping downloading", potential_file_name)
	}
}

type OutCounter struct {
	sync.Mutex
	Count    int
	DoneChan chan struct{}
}

func (oc *OutCounter) Add() {
	oc.Lock()
	oc.Count++
	oc.Unlock()
}
func (oc *OutCounter) Dec() {
	oc.Lock()
	oc.Count--
	oc.Unlock()
	if oc.Count == 0 {
		close(oc.DoneChan)
	}
}
func (oc *OutCounter) Wait() {
	if oc.DoneChan == nil {
		oc.DoneChan = make(chan struct{})
	}
	<-oc.DoneChan
}

type Url string
type UrlChannel chan Url

func NewUrlChannel() *UrlChannel {
	var itm UrlChannel
	itm = make(UrlChannel)
	return &itm
}
func UrlReceiver(chUrls UrlChannel, chan_fetch UrlChannel, out_count *OutCounter, sleep int) {
	r := rand.New(rand.NewSource(1))
	crawled_urls := make(map[Url]bool) // URLS we've crawled already so no need to revisit

	for url := range chUrls {
		fmt.Println("Receive URL to crawl", url)
		_, ok := crawled_urls[url]
		if !ok {
			fmt.Println("This url needs crawling")
			crawled_urls[url] = true
			var time_to_sleep time.Duration
			if sleep != 0 {
				time_to_sleep = time.Millisecond * time.Duration(r.Intn(sleep))
			}
			fmt.Println("Sleeping for", time_to_sleep)
			time.Sleep(time_to_sleep)
			fmt.Println("Done sleeping")
			go crawl(url, chUrls, chan_fetch, out_count)
		} else {
			out_count.Dec()
		}
	}
}
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

func main() {
	seedUrls := os.Args[1:]
	var out_count OutCounter

	// Channels
	chUrls := *NewUrlChannel()     // URLS to crawl
	chan_fetch := *NewUrlChannel() // Files to fetch

	// Kick off the seed process (concurrently)
	// Needs to be a gofunc as otherwise we get stuck here on channel send
	// As the receiver isn't started yet
	go func() {
		for _, url := range seedUrls {
			fmt.Println("Seeding :", url)
			out_count.Add()
			chUrls <- Url(url)
		}
	}()
	// This is actually the Crawler that takes URLS and spits out
	// jpg files to fetch
	// And feeds itself new URLs on the chUrls
	go UrlReceiver(chUrls, chan_fetch, &out_count, 10000)

	// This is now the fetch worker
	// Fetch up to n simultaneous items
	fetch_token_chan := make(chan bool, 16)
	for i := 0; i < 4; i++ {
		fetch_token_chan <- true
	}
	file, err := os.Create("out_items.txt")
	if err != nil {
		log.Fatal("Cannot create file", err)
	}
	defer file.Close()
	go func() {
		f, err := os.Open("in_items.txt")
		if err != nil {
			//  fmt.Printf("error opening file: %v\n",err)
			//  os.Exit(1)
			return
		}
		r := bufio.NewReader(f)
		s, e := Readln(r)
		for e == nil {
			fmt.Println("Fast adding ", s)
			chan_fetch <- Url(s)
			s, e = Readln(r)
		}
	}()
	go func() {
		fetched_urls := make(map[Url]bool)
		for fetch_url := range chan_fetch {
			_, ok := fetched_urls[fetch_url]
			if !ok {
				fetched_urls[fetch_url] = true
				<-fetch_token_chan
				go func() {
					fetch(fetch_url)
					fmt.Fprintf(file, "%s\n", string(fetch_url))
					fetch_token_chan <- true
				}()
			}
		}
	}()

	out_count.Wait()
	close(chUrls)
}
