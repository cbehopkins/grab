package grab

import (
	"fmt"
	"log"
	"os"
)

type UrlRx struct {
	// URLs come in on this channel
	// The Magic is that this channel is the input to our UrlStore
	chUrls UrlChannel
	// When we find something we want to fetch it goes out on here
	chan_fetch UrlChannel
	// WaitGroup to say we are busy/when we have finished
	OutCount *OutCounter
	// Our way of limiting our throughput - Number of Crawls happening at once
	crawl_chan TokenChan
	// We need a place to store infinitly many Urls waiting to be crawled
	UrlStore *UrlStore
	// Is the debug fiel in use/need closing
	crawl_active bool
	file         *os.File
	DbgUrls      bool
	// Mark all urls(Not just those in the current domain) as interesting
	AllInteresting bool
}

func NewUrlReceiver(chUrls UrlChannel, chan_fetch UrlChannel, out_count *OutCounter, crawl_chan TokenChan) *UrlRx {
	itm := new(UrlRx)
	// The seed URLs come in from here
	// and are sent to the UrlStore
	itm.chUrls = chUrls
	itm.chan_fetch = chan_fetch
	itm.OutCount = out_count
	itm.crawl_chan = crawl_chan
	// Tell the UrlStore to take input from the channel our seed Urls come in on
	itm.UrlStore = NewUrlStore(chUrls)
	itm.DbgUrls = true
	return itm
}
func (ur UrlRx) Start() {
	go ur.urlRxWorker()

}
func (ur *UrlRx) DbgFile(crawl_file string) {
	if crawl_file != "" {
		var err error
		ur.file, err = os.Create(crawl_file)
		if err != nil {
			log.Fatal("Cannot create file", err)
		}
		ur.crawl_active = true
	}
}
func (ur *UrlRx) SetDbgUrls(vary bool) {
	ur.DbgUrls = vary
}
func (ur *UrlRx) SetAllInteresting(vary bool) {
	ur.AllInteresting = vary
}
func (ur UrlRx) urlRxWorker() {
	crawled_urls := make(map[Url]struct{}) // URLS we've crawled already so no need to revisit
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
	for url, ok := ur.UrlStore.Pop(); ok; url, ok = ur.UrlStore.Pop() {
		_, ok := crawled_urls[url]
		if !ok {
			if ur.DbgUrls {
				fmt.Println("Receive URL to crawl", url)
			}
			//fmt.Println("This url needs crawling")
			crawled_urls[url] = struct{}{}
			//fmt.Println("Getting a crawl token")
			ur.crawl_chan.GetToken()
			//fmt.Println("Crawl token rx for:", url)
			go ur.crawl(url, err_url_chan)
			if ur.crawl_active {
				fmt.Fprintf(ur.file, "%s\n", string(url))
			}
		} else {
			ur.OutCount.Dec()
		}
	}
	close(err_url_chan)
	for bob, _ := range errored_urls {
		fmt.Println("Errored URL:", bob)
	}
	if ur.crawl_active {
		defer ur.file.Close()
	}
}