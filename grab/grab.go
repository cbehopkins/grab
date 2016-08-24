package grab

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
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
func crawl(urli Url, ch UrlChannel, fetch_chan UrlChannel, out_count *OutCounter, errored_urls UrlChannel) {

	resp, err := http.Get(string(urli))
	defer out_count.Dec()

	if err != nil {

		fmt.Println("ERROR: Failed to crawl \"" + urli + "\"")
		// TBD add in error queue to try again with at end of crawl.
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
		case io.EOF:
			fmt.Println("EOF error found")
		default:
			switch err.(type) {
			case *url.Error:
				fmt.Println("URL Error")
			default:
				fmt.Printf("Error type is %T, %#v\n", err, err)
				panic(err)
			}
		}
		errored_urls <- urli
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
			ok, urlj := getHref(t)
			if !ok {
				continue
			}

			// Make sure the url begines in http**
			hasProto := strings.Index(urlj, "http") == 0
			if hasProto {

				is_jpg := strings.Contains(urlj, ".jpg")
				if is_jpg {
					fetch_chan <- Url(urlj)
				} else {
					arrayi := strings.Split(string(urli), "/")
					arrayj := strings.Split(string(urlj), "/")
					domain_i := arrayi[2]
					domain_j := arrayj[2]
					if domain_i == domain_j {
						//fmt.Printf("Interesting url, %s, %s, %s\n", domain_i, domain_j, urlj)
						out_count.Add()
						ch <- Url(urlj)
					} else {
						//fmt.Printf("Uninteresting url, %s, %s, %s\n", domain_i, domain_j, urlj)
					}
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
func Fetch(fetch_url Url) bool {
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
		return true
	} else {
		fmt.Println("skipping downloading", potential_file_name)
		return false
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
	data          []Url
	PushChannel   chan Url
	PopChannel    chan Url
	enforce_order bool
}

func NewUrlStore(in_if_arr ...interface{}) *UrlStore {
	itm := new(UrlStore)
	itm.data = make([]Url, 0, 1024)
	itm.PopChannel = make(chan Url, 2)
	for _, in_if := range in_if_arr {
		switch in_if.(type) {
		case chan Url:
			itm.PushChannel = in_if.(chan Url)
		case UrlChannel:
			itm.PushChannel = in_if.(UrlChannel)
		case bool:
			// A bool type is controlling our ordering attribute
			itm.enforce_order = in_if.(bool)
		case nil:
			//Nothing to do
		default:
			fmt.Printf("Type is %T\n", in_if)
		}
	}

	if itm.PushChannel == nil {
		itm.PushChannel = make(chan Url, 2)
	}

	go itm.urlWorker()
	return itm
}
func (us *UrlStore) urlWorker() {
	var tmp_chan chan Url
	in_chan := us.PushChannel
	var input_channel_closed bool
	backup_store := us.data
	var tmp_val Url
	for {
		if len(us.data) > 0 {
			tmp_chan = us.PopChannel
			if us.enforce_order {
				tmp_val = us.data[0]
			} else {
				tmp_val = us.data[len(us.data)-1]
			}
		} else {
			tmp_chan = nil
			if input_channel_closed == true {
				//fmt.Println("Channel found closed")
				close(us.PopChannel)
				return
			}
		}
		select {
		case ind, ok := <-in_chan:
			if ok {
				//fmt.Println("Queueing URL :", ind)
				if us.enforce_order {
					if len(us.data) < cap(us.data) {
						// Safe to use append here as it won't do the copy as there is capacity
						us.data = append(us.data, ind)
					} else {
						if cap(backup_store) > cap(us.data) {
							// here we copy into the backup store's data storage
							// the current data in the data store
							//ncopy(backup_store[0:len(us.data)], us.data[0:len(us.data)])
							// now stick the data on the end
							backup_store = append(us.data, ind)
							us.data = backup_store
							//us.data = append(us.data, ind)
						} else {
							// This is the case where we need to grow the size of the store
							us.data = append(us.data, ind)
							backup_store = us.data
						}
					}
				} else {
					//in the normal case we just chuck the data on the end
					us.data = append(us.data, ind)
				}
				// activate the pop channel
				tmp_chan = us.PopChannel
			} else {
				//chanel is closed
				//fmt.Println("Channel is closed")
				in_chan = nil
				input_channel_closed = true
			}

		case tmp_chan <- tmp_val:
			//fmt.Println("DeQueueing URL", tmp_val)
			if us.enforce_order {
				//copy(us.data[:len(us.data)-1],us.data[1:len(us.data)])
				//us.data = us.data[:len(us.data)-1]
				// Reduce the length and capacity
				us.data = us.data[1:len(us.data)]
			} else {
				// take one off the length without reducing the capacity
				us.data = us.data[:len(us.data)-1]
			}
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
	fmt.Println("Popped URL:", itm)
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
	errored_urls := make(map[Url]bool)
	err_url_chan := make(UrlChannel)
	go func() {
		for bob := range err_url_chan {
			errored_urls[bob] = true
		}
	}()
	url_store := NewUrlStore(chUrls)
	for url, ok := url_store.Pop(); ok; url, ok = url_store.Pop() {
		_, ok := crawled_urls[url]
		if !ok {
			fmt.Println("Receive URL to crawl", url)
			//fmt.Println("This url needs crawling")
			crawled_urls[url] = true
			//fmt.Println("Getting a crawl token")
			<-crawl_chan
			//fmt.Println("Crawl token rx for:", url)
			go crawl(url, chUrls, chan_fetch, out_count, err_url_chan)
		} else {
			out_count.Dec()
		}
	}
	close(err_url_chan)
	for bob, _ := range errored_urls {
		fmt.Println("Errored URL:", bob)
	}
}
