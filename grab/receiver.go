package grab

import (
	"fmt"
	"log"
	"os"
	"time"
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
	crawl_chan *TokenChan
	// We need a place to store infinitly many Urls waiting to be crawled
	UrlStore *UrlStore
	// Is the debug fiel in use/need closing
	crawl_active bool
	file         *os.File
	DbgUrls      bool
	Domv         *DomVisit
	// Mark all urls(Not just those in the current domain) as interesting
	AllInteresting bool
}

func NewUrlReceiver(chUrls UrlChannel, chan_fetch UrlChannel, out_count *OutCounter, crawl_chan *TokenChan) *UrlRx {
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
	itm.Domv = NewDomVisit()
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
	crawled_urls := NewUrlCache() // URLS we've crawled already so no need to revisit
	// It's bad news if the chUrls blocks as then:
	// the crawl func can't add new URLS
	// So it stops mid crawl
	// Yet new crawls are still started
	// Somewhere we need an infinite bufer to absorb all the incoming URLs
	// This could be on the stack of crawl function instances, or:
	hm := NewHamster(true, // Promiscuous
		false, // Shallow
		ur.AllInteresting,
		ur.DbgUrls)
	hm.SetDv(ur.Domv)
	hm.SetFetchCh(ur.chan_fetch)
	hm.SetOc(ur.OutCount)
	hm.SetGrabCh(ur.chUrls)

	for urly, ok := ur.UrlStore.Pop(); ok; urly, ok = ur.UrlStore.Pop() {
		ok := crawled_urls.Query(urly)
		if !ok {
			//if ur.DbgUrls {
			//	fmt.Println("Receive URL to crawl", url)
			//}
			//fmt.Println("This url needs crawling")
			token_got := GetBase(string(urly))

			crawled_urls.Add(urly)
			// Annoyingly there is a tendancy for URLs for one site to clump together
			// So we peruse through the list until we find a Url we can immediatly use
			if ur.crawl_chan.TryGetToken(token_got) {
				//fmt.Println("Crawl token rx for:", token_got)
				go hm.GrabT(urly, token_got,
					ur.crawl_chan,
				)
				if ur.crawl_active {
					fmt.Fprintf(ur.file, "%s\n", string(urly))
				}
			} else {
				// We could not immediatly get a token for this
				// put it back on the queue
				ur.chUrls <- urly
				// and wait a little while before starting up again
				// to stop us using 100% of cpu
				time.Sleep(time.Duration(10) * time.Millisecond)
			}
		} else {
			ur.OutCount.Dec()
		}
	}
	if ur.crawl_active {
		defer ur.file.Close()
	}
}
