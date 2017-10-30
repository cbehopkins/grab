package grab

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
)

// DomVisit - Domains Visited structure
type DomVisit struct {
	sm             *sync.RWMutex
	wg             *sync.WaitGroup
	domainsVisited *map[string]struct{}
	badDomains     *map[string]struct{}
	badFile        *os.File
	re             *regexp.Regexp
}

// NewDomVisit - create a new domains to visit structure
// specify a filename to sgtore the bad nameds at
func NewDomVisit(badFname string) *DomVisit {
	var itm *DomVisit
	itm = new(DomVisit)
	itm.sm = new(sync.RWMutex)
	dv := make(map[string]struct{})
	bd := make(map[string]struct{})
	itm.wg = new(sync.WaitGroup)
	itm.re = regexp.MustCompile("([a-zA-Z0-9_\\-\\.]*?)([a-zA-Z0-9_\\-]+\\.\\w+)/?$")
	itm.domainsVisited = &dv
	itm.badDomains = &bd
	var err error
	itm.badFile, err = os.OpenFile("badf.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	check(err)
	itm.LoadBadFiles(badFname)
	return itm
}

// WaitLoad - wait until the load has completed
func (dv *DomVisit) WaitLoad() {
	dv.wg.Wait()
}

// LoadBadFiles - load in existing bad names
func (dv *DomVisit) LoadBadFiles(badURLFn string) {
	dv.wg.Add(1) // one for badUrls
	go func() {
		badURLChan := *NewURLChannel()
		go LoadFile(badURLFn, badURLChan, nil, true, false)
		for itm := range badURLChan {
			dv.AddBad(itm.URL())
		}
		dv.wg.Done()
	}()
}

// Close down the visitor
func (dv DomVisit) Close() {
	dv.badFile.Close()
}

// Exist returns true if the supplied string already exists
func (dv DomVisit) Exist(str string) bool {
	str = dv.baseIt(str)
	dv.sm.RLock()
	tdv := *dv.domainsVisited
	_, ok := tdv[str]
	dv.sm.RUnlock()
	return ok
}
func (dv DomVisit) baseIt(str string) string {
	if str == "" {
		return ""
	}
	// This MUST only have the basename passed to it
	t1 := dv.re.FindStringSubmatch(str)
	var base string
	if len(t1) > 2 {
		base = t1[2]
	} else {
		fmt.Printf("DomVisit Failed to parse:\"%s\"\n%v\n", str, t1)
		//panic(str)
	}
	return base
}

// Add a domain to the allowed to visit
func (dv DomVisit) Add(strt string) {
	str := dv.baseIt(strt)
	log.Printf("Adding:%s, because %s\n", str, strt)
	dv.sm.Lock()
	tdv := *dv.domainsVisited
	tdv[str] = struct{}{}
	dv.sm.Unlock()
}

// AddBad adds a bad domain that we should not visit
func (dv DomVisit) AddBad(strt string) {
	str := dv.baseIt(strt)
	log.Printf("Adding Bad:%s, because %s\n", str, strt)
	dv.sm.Lock()
	tdv := *dv.badDomains
	tdv[str] = struct{}{}
	dv.sm.Unlock()
}
func (dv DomVisit) multiURL(urlIn URL) bool {
	tmpMap := make(map[string]int)
	array := strings.Split(urlIn.URL(), "/")
	for _, v := range array {
		cnt, ok := tmpMap[v]
		if ok {
			// if we see the same url fragment multiple times
			// Then something weird is going on
			if cnt > 2 {
				return true
			}
			tmpMap[v] = cnt + 1

		} else {
			tmpMap[v] = 1
		}
	}
	return false
}

// GoodURL Parses a URL to determine if it si good
func (dv DomVisit) GoodURL(urlIn URL) bool {
	mc := dv.multiCheck(urlIn)
	if !mc {
		return false
	}

	return dv.reference(urlIn)
}

func (dv DomVisit) isOutPhp(urlIn URL) bool {
	return strings.Contains(urlIn.URL(), "/out.php?")
}

func (dv DomVisit) multiCheck(urlIn URL) bool {

	// Is this a URL we want to visit
	// Many reasons we may not want to visit it
	// We maintain a list of banned domains
	// The URL could also use one of the tricks
	// to
	bn := urlIn.Base()
	ub := dv.baseIt(bn)
	bdp := *dv.badDomains
	_, ok := bdp[ub]
	if ok {
		return false
	}

	ok = dv.multiURL(urlIn)
	if ok {
		return false
	}

	ok = dv.isOutPhp(urlIn)
	if ok {
		return false
	}

	return true
}
func (dv DomVisit) reference(urlIn URL) bool {
	// we get a lot of urls that are:
	// something.php?somehere.com
	// I'm usually not interested in those
	// so let's have a function to detect and screen them
	u, err := url.Parse(urlIn.URL()) // REVISIT
	if err != nil {
		// If we can't parse the url then it's a bad url
		return false
	}
	query := u.RawQuery
	opq := u.Path
	icom := strings.Contains(query, ".com")
	iphp := strings.HasSuffix(opq, "out.php")
	if iphp && icom {
		fmt.Fprintf(dv.badFile, "%v\n", urlIn)
		return false
	}
	return true
}

// VisitedQ Asks the Question
// Have we visted here
func (dv DomVisit) VisitedQ(urlIn string) bool {
	ok := dv.Exist(urlIn)
	if ok {
		return true
	}
	return false
}

// VisitedA Adds a domain we should visit
func (dv DomVisit) VisitedA(urlIn string) bool {
	//ok := dv.Exist(url_in)
	// This is a costly process, so do it once for Exist and Add
	str := dv.baseIt(urlIn)
	dv.sm.RLock()
	tdv := *dv.domainsVisited
	_, ok := tdv[str]
	dv.sm.RUnlock()

	if ok {
		return true
	}
	//fmt.Println("New Authorised Domain:", url_in)
	//dv.Add(url_in)
	//log.Printf("Adding:%s, because %s\n", str, url_in)
	dv.sm.Lock()
	tdv = *dv.domainsVisited
	tdv[str] = struct{}{}
	dv.sm.Unlock()
	return false

}

// DomVisitI shows the functions required by a domain visitor
type DomVisitI interface {
	GoodURL(URL) bool
	VisitedQ(string) bool
	VisitedA(string) bool
	Seed(string, bool) chan URL
}

// Seed will seed the domains we're allowed to visit
// from a filename and write it to the
func (dv DomVisit) Seed(urlFn string, promiscuous bool) chan URL {
	srcURLChan := make(chan URL)
	go func() {
		seedURLChan := *NewURLChannel()
		go LoadFile(urlFn, seedURLChan, nil, true, false)
		s := Spinner{}
		cnt := 0
		for itm := range seedURLChan {
			if false {
				fmt.Println("SeedURL:", itm)
			}
			if itm.Initialise() {
				log.Fatal("URL needed initialising in nd", itm)
			}
			domainI := itm.Base()
			if domainI != "" {
				// Mark this as a domain we can Fetch from
				_ = dv.VisitedA(domainI)
				// send this URL for grabbing
				if promiscuous {
					itm.SetPromiscuous()
				}
				itm.SetShallow()
				s.PrintSpin(cnt)
				srcURLChan <- itm
				cnt++
				//fmt.Println(itm, "Sent")
			}
		}
		fmt.Println("seed_url_chan seen closed")
		close(srcURLChan)
	}()
	return srcURLChan
}
