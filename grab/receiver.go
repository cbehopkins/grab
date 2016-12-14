package grab

import (
	"fmt"
	"log"
	"os"
)

type UrlRx struct {
	chUrls          UrlChannel
	chan_fetch      UrlChannel
	out_count       *OutCounter
	crawl_chan      TokenChan
	UrlStore        *UrlStore
	crawl_active    bool
	file            *os.File
	dbg_urls        bool
	all_interesting bool
}

func NewUrlReceiver(chUrls UrlChannel, chan_fetch UrlChannel, out_count *OutCounter, crawl_chan TokenChan) *UrlRx {
	itm := new(UrlRx)
	itm.chUrls = chUrls
	itm.chan_fetch = chan_fetch
	itm.out_count = out_count
	itm.crawl_chan = crawl_chan
	itm.UrlStore = NewUrlStore(chUrls)
	itm.dbg_urls = true
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
func (ur *UrlRx) DbgUrls(vary bool) {
	ur.dbg_urls = vary
}
func (ur *UrlRx) AllInteresting(vary bool) {
	ur.all_interesting = vary
}
func (ur UrlRx) urlRxWorker() {
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
	for url, ok := ur.UrlStore.Pop(); ok; url, ok = ur.UrlStore.Pop() {
		_, ok := crawled_urls[url]
		if !ok {
			fmt.Println("Receive URL to crawl", url)
			//fmt.Println("This url needs crawling")
			crawled_urls[url] = true
			//fmt.Println("Getting a crawl token")
			ur.crawl_chan.GetToken()
			//fmt.Println("Crawl token rx for:", url)
			go crawl(url, ur.chUrls, ur.chan_fetch, ur.out_count, err_url_chan, ur.dbg_urls, ur.all_interesting)
			if ur.crawl_active {
				fmt.Fprintf(ur.file, "%s\n", string(url))
			}
		} else {
			ur.out_count.Dec()
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
