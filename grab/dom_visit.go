package grab

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
)

// Domains Visited structure
type DomVisit struct {
	sm              *sync.Mutex
	domains_visited *map[string]struct{}
	bad_domains     *map[string]struct{}
}

func NewDomVisit() *DomVisit {
	var itm *DomVisit
	itm = new(DomVisit)
	itm.sm = new(sync.Mutex)
	dv := make(map[string]struct{})
	bd := make(map[string]struct{})
	itm.domains_visited = &dv
	itm.bad_domains = &bd
	return itm
}
func (dv DomVisit) Exist(str string) bool {
	str = base_it(str)
	dv.sm.Lock()
	tdv := *dv.domains_visited
	_, ok := tdv[str]
	dv.sm.Unlock()
	return ok
}
func base_it(str string) string {
	if str == "" {
		return ""
	}
	// This MUST only have the basename passed to it
	re := regexp.MustCompile("([a-zA-Z0-9_\\-\\.]*?)([a-zA-Z0-9_\\-]+\\.\\w+)/?$")
	t1 := re.FindStringSubmatch(str)
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
	str := base_it(strt)
	log.Printf("Adding:%s, because %s\n", str, strt)
	dv.sm.Lock()
	tdv := *dv.domains_visited
	tdv[str] = struct{}{}
	dv.sm.Unlock()
}
func (dv DomVisit) AddBad(strt string) {
	str := base_it(strt)
	log.Printf("Adding Bad:%s, because %s\n", str, strt)
	dv.sm.Lock()
	tdv := *dv.bad_domains
	tdv[str] = struct{}{}
	dv.sm.Unlock()
}
func (dv DomVisit) multi_url(url_in string) bool {
	tmp_map := make(map[string]int)
	array := strings.Split(url_in, "/")
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

func (dv DomVisit) GoodUrl(url_in string) bool {
	// Is this a URL we want to visit
	// Many reasons we may not want to visit it
	// We maintain a list of banned domains
	// The URL could also use one of the tricks
	// to
	bn := GetBase(url_in)
	ub := base_it(bn)
	bdp := *dv.bad_domains
	_, ok := bdp[ub]
	if ok {
		return false
	}

	ok = dv.multi_url(url_in)
	if ok {
		return false
	}
	return true
}

func (dv DomVisit) VisitedQ(url_in string) bool {
	ok := dv.Exist(url_in)
	if ok {
		//log.Println("Already Visited:", url_in)
		return true
	} else {
		return false
	}
}
func (dv DomVisit) VisitedA(url_in string) bool {
	ok := dv.Exist(url_in)
	if ok {
		return true
	} else {
		//mt.Println("New Authorised Domain:", url_in)
		dv.Add(url_in)
		return false
	}
}

type DomVisitI interface {
	GoodUrl(url_in string) bool
	VisitedQ(url_in string) bool
	VisitedA(url_in string) bool
}
