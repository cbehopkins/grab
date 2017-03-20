package grab

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type DomVisit struct {
	sync.Mutex
	domains_visited map[string]struct{}
}

func NewDomVisit() *DomVisit {
	var itm *DomVisit
	itm = new(DomVisit)
	itm.domains_visited = make(map[string]struct{})
	return itm
}
func (dv *DomVisit) Exist(str string) bool {
	dv.Lock()
	_, ok := dv.domains_visited[str]
	dv.Unlock()
	return ok
}
func (dv *DomVisit) Add(str string) {
	dv.Lock()
	dv.domains_visited[str] = struct{}{}
	dv.Unlock()
}

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
func (ur UrlRx) VisitedQ(url_in string) bool {
	ok := ur.Domv.Exist(url_in)
	if ok {
		return true
	} else {
		return false
	}
}
func (ur UrlRx) VisitedA(url_in string) bool {
	ok := ur.Domv.Exist(url_in)
	if ok {
		return true
	} else {
		//fmt.Println("New Authorised Domain:", url_in)
		ur.Domv.Add(url_in)
		return false
	}
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

type UrlCache struct {
	sync.Mutex
	uc map[Url]time.Time
}

func NewUrlCache() *UrlCache {
	itm := new(UrlCache)
	itm.uc = make(map[Url]time.Time)
	go itm.shrinker()
	return itm
}
func (uc *UrlCache) shrink() {
	uc.Lock()
	ct := time.Now()
	to_delete := make(map[Url]struct{})
	for key, rct := range uc.uc {
		// If the current time - last time the url was accessed
		// is > 5 minutes then delete that Url

		if (ct.Sub(rct)) > (5 * time.Minute) {
			to_delete[key] = struct{}{}
		}
	}

	for key, _ := range to_delete {
		delete(uc.uc, key)
	}
	uc.Unlock()
}
func (uc *UrlCache) shrinker() {

	for {
		// Tidy things up
		time.AfterFunc(10*time.Minute, uc.shrink)
	}
}
func (uc *UrlCache) Query(url Url) bool {
	uc.Lock()
	_, ok := uc.uc[url]
	// Update to say we have accessed it
	if ok {
		uc.uc[url] = time.Now()
	}
	uc.Unlock()
	return ok
}
func (uc *UrlCache) Add(url Url) {
	uc.Lock()
	uc.uc[url] = time.Now()
	uc.Unlock()
}
func (ur UrlRx) urlRxWorker() {
	crawled_urls := NewUrlCache() // URLS we've crawled already so no need to revisit
	// It's bad news if the chUrls blocks as then:
	// the crawl func can't add new URLS
	// So it stops mid crawl
	// Yet new crawls are still started
	// Somewhere we need an infinite bufer to absorb all the incoming URLs
	// This could be on the stack of crawl function instances, or:
	err_url_chan := make(UrlChannel)
	go func() {
		for _ = range err_url_chan {
		}
	}()
	for url, ok := ur.UrlStore.Pop(); ok; url, ok = ur.UrlStore.Pop() {
		ok := crawled_urls.Query(url)
		if !ok {
			//if ur.DbgUrls {
			//	fmt.Println("Receive URL to crawl", url)
			//}
			//fmt.Println("This url needs crawling")
			token_got := getBase(string(url))

			crawled_urls.Add(url)
			// Annoyingly there is a tendancy for URLs for one site to clump together
			// So we peruse through the list until we find a Url we can immediatly use
			if ur.crawl_chan.TryGetToken(token_got) {
				//fmt.Println("Crawl token rx for:", token_got)
				go ur.crawl(url, err_url_chan, token_got)
				if ur.crawl_active {
					fmt.Fprintf(ur.file, "%s\n", string(url))
				}
			} else {
				// We could not immediatly get a token for this
				// put it back on the queue
				ur.chUrls <- url
				// and wait a little while before starting up again
				// to stop us using 100% of cpu
				time.Sleep(time.Duration(10) * time.Millisecond)
			}
		} else {
			ur.OutCount.Dec()
		}
	}
	close(err_url_chan)
	if ur.crawl_active {
		defer ur.file.Close()
	}
}
