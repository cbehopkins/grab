package grab

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"

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
func Fetch(fetch_url Url) {
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

type UrlStore struct {
	sync.Mutex
	data        []Url
	PushChannel chan Url
	PopChannel  chan Url
}

func NewUrlStore() *UrlStore {
	itm := new(UrlStore)
	itm.data = make([]Url, 0, 1024)
	itm.PushChannel = make(chan Url, 2)
	itm.PopChannel = make(chan Url, 2)
	go itm.urlWorker()
	return itm
}
func (us *UrlStore) urlWorker() {
	var tmp_chan chan Url
	in_chan := us.PushChannel
	var input_channel_closed bool
	var tmp_val Url
	for {
		if len(us.data) > 0 {
			tmp_chan = us.PopChannel
			tmp_val = us.data[len(us.data)-1]
		} else {
			tmp_chan = nil
			if input_channel_closed == true {
				fmt.Println("Channel found closed")
				close(us.PopChannel)
				return
			}
		}
		select {
		case ind, ok := <-in_chan:
			if ok {
				us.data = append(us.data, ind)
				us.data[len(us.data)-1] = ind
				tmp_chan = us.PopChannel
			} else {
				//chanel is closed
				fmt.Println("Channel is closed")	
	in_chan = nil
				input_channel_closed = true
			}

		case tmp_chan <- tmp_val:
			us.data = us.data[:len(us.data)-1]
		}
	}
}

func (us UrlStore) Close() {
close(us.PushChannel)
}
func (us UrlStore) Add(itm Url) {
	us.PushChannel <- itm
}

func (us UrlStore) Pop() (itm Url, ok bool) {
	itm, ok = <-us.PopChannel
	return
}
func UrlReceiver(chUrls UrlChannel, chan_fetch UrlChannel, out_count *OutCounter, crawl_chan chan struct{}) {
	crawled_urls := make(map[Url]bool) // URLS we've crawled already so no need to revisit
	// It's bad news if the chUrls blocks as then:
	// the crawl func can't add new URLS
	// So it stops mid crawl
	// Yet new crawls are still started
	// Somewhere we need an infinite bufer to absorb all the incoming URLs
	// This could be on the stack of crawl function instances, or:

	url_store := NewUrlStore()
	// Receive from chUrls and store in a temporary buffer
	go func () {for url := range chUrls {
		url_store.Add(url)
	}}()
	for url, ok := url_store.Pop(); ok; url, ok = url_store.Pop() {
		fmt.Println("Receive URL to crawl", url)
		_, ok := crawled_urls[url]
		if !ok {
			fmt.Println("This url needs crawling")
			crawled_urls[url] = true
			<-crawl_chan
			go crawl(url, chUrls, chan_fetch, out_count)
		} else {
			out_count.Dec()
		}
	}
}
