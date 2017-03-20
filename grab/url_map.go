package grab

import (
	"fmt"
	"sync"
)

type UrlMap struct {
	sync.RWMutex
	mp map[Url]struct{}
}

func NewUrlMap() *UrlMap {
	itm := new(UrlMap)
	itm.mp = make(map[Url]struct{})
	return itm
}

func (um UrlMap) Exist(key Url) bool {
	um.RLock()
	_, ok := um.mp[key]
	um.RUnlock()
	return ok
}
func (um UrlMap) Set(key Url) {
	um.Lock()
	um.mp[key] = struct{}{}
	um.Unlock()
}
func (um UrlMap) Size() int {
	um.RLock()
	defer um.RUnlock()
	return len(um.mp)
}

// Return a channel that we can read the (current) list of Urls From
// If we start to worry about memory usage then as long as we return some Urls,
// we don't have to worry about returning all of them
func (um UrlMap) Visit() chan Url {
	ret_chan := make(chan Url)
	fmt.Println("Called Visit")
	go func() {
		fmt.Println("Grabbing Lock")
		um.RLock()
		fmt.Println("Got Lock")
		tmp_slice := make([]Url, len(um.mp))
		i := 0
		for v, _ := range um.mp {
			tmp_slice[i] = v
			i++
		}
		um.RUnlock()
		fmt.Println("Sending Values")
		for _, v := range tmp_slice {
			ret_chan <- v
		}
		fmt.Println("Closing Visit Chan")
		close(ret_chan)
	}()
	return ret_chan
}
func (um UrlMap) Delete(key Url) {
	delete(um.mp, key)
}
