package grab

import "sync"

type DomVisit struct {
	sm              *sync.Mutex
	domains_visited *map[string]struct{}
}

func NewDomVisit() *DomVisit {
	var itm *DomVisit
	itm = new(DomVisit)
	itm.sm = new(sync.Mutex)
	dv := make(map[string]struct{})

	itm.domains_visited = &dv
	return itm
}
func (dv DomVisit) Exist(str string) bool {
	dv.sm.Lock()
	tdv := *dv.domains_visited
	_, ok := tdv[str]
	dv.sm.Unlock()
	return ok
}
func (dv DomVisit) Add(str string) {
	dv.sm.Lock()
	tdv := *dv.domains_visited
	tdv[str] = struct{}{}
	dv.sm.Unlock()
}
func (dv DomVisit) VisitedQ(url_in string) bool {
	ok := dv.Exist(url_in)
	if ok {
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
		//fmt.Println("New Authorised Domain:", url_in)
		dv.Add(url_in)
		return false
	}
}

type DomVisitI interface {
	VisitedQ(url_in string) bool
	VisitedA(url_in string) bool
}
