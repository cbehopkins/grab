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

// Domains Visited structure
type DomVisit struct {
	sm              *sync.RWMutex
	domains_visited *map[string]struct{}
	bad_domains     *map[string]struct{}
	bad_file        *os.File
	re              *regexp.Regexp
}

func NewDomVisit() *DomVisit {
	var itm *DomVisit
	itm = new(DomVisit)
	itm.sm = new(sync.RWMutex)
	dv := make(map[string]struct{})
	bd := make(map[string]struct{})

	itm.re = regexp.MustCompile("([a-zA-Z0-9_\\-\\.]*?)([a-zA-Z0-9_\\-]+\\.\\w+)/?$")
	itm.domains_visited = &dv
	itm.bad_domains = &bd
	var err error
	itm.bad_file, err = os.OpenFile("badf.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	check(err)
	return itm
}
func (dv DomVisit) Close() {
	dv.bad_file.Close()
}
func (dv DomVisit) Exist(str string) bool {
	str = dv.base_it(str)
	dv.sm.RLock()
	tdv := *dv.domains_visited
	_, ok := tdv[str]
	dv.sm.RUnlock()
	return ok
}
func (dv DomVisit) base_it(str string) string {
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

func (dv DomVisit) Add(strt string) {
	str := dv.base_it(strt)
	log.Printf("Adding:%s, because %s\n", str, strt)
	dv.sm.Lock()
	tdv := *dv.domains_visited
	tdv[str] = struct{}{}
	dv.sm.Unlock()
}
func (dv DomVisit) AddBad(strt string) {
	str := dv.base_it(strt)
	log.Printf("Adding Bad:%s, because %s\n", str, strt)
	dv.sm.Lock()
	tdv := *dv.bad_domains
	tdv[str] = struct{}{}
	dv.sm.Unlock()
}
func (dv DomVisit) multi_url(url_in Url) bool {
	tmp_map := make(map[string]int)
	array := strings.Split(url_in.Url(), "/")
	for _, v := range array {
		cnt, ok := tmp_map[v]
		if ok {
			// if we see the same url fragment multiple times
			// Then something weird is going on
			if cnt > 2 {
				return true
			} else {
				tmp_map[v] = cnt + 1
			}
		} else {
			tmp_map[v] = 1
		}
	}
	return false
}

func (dv DomVisit) GoodUrl(url_in Url) bool {
	mc := dv.multiCheck(url_in)
	if !mc {
		return false
	}

	return dv.reference(url_in)
}

func (dv DomVisit) isOutPhp(url_in Url) bool {
	return strings.Contains(url_in.Url(), "/out.php?")
}

func (dv DomVisit) multiCheck(url_in Url) bool {

	// Is this a URL we want to visit
	// Many reasons we may not want to visit it
	// We maintain a list of banned domains
	// The URL could also use one of the tricks
	// to
	bn := url_in.Base()
	ub := dv.base_it(bn)
	bdp := *dv.bad_domains
	_, ok := bdp[ub]
	if ok {
		return false
	}

	ok = dv.multi_url(url_in)
	if ok {
		return false
	}

	ok = dv.isOutPhp(url_in)
	if ok {
		return false
	}

	return true
}
func (dv DomVisit) reference(url_in Url) bool {
	// we get a lot of urls that are:
	// something.php?somehere.com
	// I'm usually not interested in those
	// so let's have a function to detect and screen them
	u, err := url.Parse(url_in.Url()) // REVISIT
	if err != nil {
		// If we can't parse the url then it's a bad url
		return false
	}
	query := u.RawQuery
	opq := u.Path
	icom := strings.Contains(query, ".com")
	iphp := strings.HasSuffix(opq, "out.php")
	if iphp && icom {
		fmt.Fprintf(dv.bad_file, "%v\n", url_in)
		return false
	}
	return true
}

// VisitedQ Asks the Question
// Have we visted here
func (dv DomVisit) VisitedQ(url_in string) bool {
	ok := dv.Exist(url_in)
	if ok {
		//log.Println("Already Visited:", url_in)
		return true
	} else {
		return false
	}
}

// VisitedA Adds a domain we should visit
func (dv DomVisit) VisitedA(url_in string) bool {
	//ok := dv.Exist(url_in)
	// This is a costly process, so do it once for Exist and Add
	str := dv.base_it(url_in)
	dv.sm.RLock()
	tdv := *dv.domains_visited
	_, ok := tdv[str]
	dv.sm.RUnlock()

	if ok {
		return true
	} else {
		//fmt.Println("New Authorised Domain:", url_in)
		//dv.Add(url_in)
		//log.Printf("Adding:%s, because %s\n", str, url_in)
		dv.sm.Lock()
		tdv := *dv.domains_visited
		tdv[str] = struct{}{}
		dv.sm.Unlock()
		return false
	}
}

type DomVisitI interface {
	GoodUrl(url_in Url) bool
	VisitedQ(url_in string) bool
	VisitedA(url_in string) bool
}
