package grab

import (
	"sync"

	"time"
)

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
