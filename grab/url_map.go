package grab

import (
	//"fmt"
	"sync"
	"time"
)

type UrlMap struct {
	sync.RWMutex
	disk_lock sync.Mutex
	mp        map[Url]struct{}
	use_disk  bool
	dkst      *DkStore
}

func NewUrlMap(filename string, overwrite bool) *UrlMap {
	var use_disk bool
	if filename != "" {
		use_disk = true
	}
	itm := new(UrlMap)
	itm.use_disk = use_disk
	if use_disk {
		itm.dkst = NewDkStore(filename, overwrite)
		go itm.flusher()
	} else {
		itm.mp = make(map[Url]struct{})
	}
	return itm
}

func (um *UrlMap) Flush() {
	um.Lock()
	um.dkst.Flush()
	um.Unlock()

}
func (um *UrlMap) Sync() {
	um.disk_lock.Lock()
	um.dkst.Sync()
	um.disk_lock.Unlock()

}
func (um *UrlMap) Close() {
	um.disk_lock.Lock()
	um.dkst.Sync()
	um.dkst.Close()
	um.disk_lock.Unlock()

}

func (um *UrlMap) flusher() {
	for {
		time.Sleep(100 * time.Second)
		um.Flush()
		um.Sync()
	}
}
func (um *UrlMap) Exist(key Url) bool {
	um.RLock()
	var ok bool
	if um.use_disk {
		ok = um.dkst.Exist(key)
	} else {
		_, ok = um.mp[key]
	}
	um.RUnlock()
	return ok
}
func (um *UrlMap) Set(key Url) {
	//fmt.Println("Get lock for:",key)
	um.Lock()
	//fmt.Println("Got Lock")
	if um.use_disk {
		um.dkst.SetAny(key, "")
	} else {
		um.mp[key] = struct{}{}
	}
	um.Unlock()
	//fmt.Println("Lock Returned for:",key)
}
func (um *UrlMap) Check(key Url) bool {
	// Be 100% sure things work as we expect!
	src_chan := um.VisitAll()
	found := false
	for v := range src_chan {
		if v == key {
			found = true
		}
	}
	return found
}

func (um *UrlMap) Size() int {
	um.RLock()
	defer um.RUnlock()
	if um.use_disk {
		return um.dkst.Size()
	}
	return len(um.mp)

}

// Return a channel that we can read the (current) list of Urls From
// If we start to worry about memory usage then as long as we return some Urls,
// we don't have to worry about returning all of them
func (um *UrlMap) VisitAll() chan Url {
	ret_chan := make(chan Url)
	//fmt.Println("Called Visit")

	go func() {
		//fmt.Println("Grabbing Lock")
		um.RLock()
		//fmt.Println("Got Lock")
		if um.use_disk {
			for v := range um.dkst.GetStringKeys() {
				//fmt.Println("Visit Url:", v)
				ret_chan <- Url(v)
			}
			um.RUnlock()

		} else {
			tmp_slice := make([]Url, len(um.mp))
			i := 0
			for v, _ := range um.mp {
				tmp_slice[i] = v
				i++
			}
			um.RUnlock()
			//fmt.Println("Sending Values")
			for _, v := range tmp_slice {
				ret_chan <- v
			}
		}
		//fmt.Println("Closing Visit Chan")
		close(ret_chan)
	}()
	return ret_chan
}
func (um *UrlMap) Visit() chan Url {
	ret_chan := make(chan Url)
	//fmt.Println("Called Visit")

	go func() {
		//fmt.Println("Grabbing Lock")
		um.RLock()
		//fmt.Println("Got Lock")
		if um.use_disk {
			string_array := um.dkst.GetStringKeysArray(100)
			um.RUnlock()
			for _, v := range string_array {
				//fmt.Println("Visit Url:", v)
				ret_chan <- Url(v)
			}

		} else {
			tmp_slice := make([]Url, len(um.mp))
			i := 0
			for v, _ := range um.mp {
				tmp_slice[i] = v
				i++
			}
			um.RUnlock()
			//fmt.Println("Sending Values")
			for _, v := range tmp_slice {
				ret_chan <- v
			}
		}
		//fmt.Println("Closing Visit Chan")
		close(ret_chan)
	}()
	return ret_chan
}

func (um *UrlMap) VisitMissing(refr *TokenChan) chan Url {
	ret_chan := make(chan Url)
	//fmt.Println("Called Visit")

	go func() {
		//fmt.Println("Grabbing Lock")
		um.RLock()
		//fmt.Println("Got Lock")
		if um.use_disk {
			// Get up to 100 things that aren't on the TokenChan
			string_array := um.dkst.GetMissing(100, refr)
			um.RUnlock()
			for _, v := range string_array {
				//fmt.Println("Visit Url:", v)
				ret_chan <- Url(v)
			}

		} else {
			tmp_slice := make([]Url, len(um.mp))
			i := 0
			for v, _ := range um.mp {
				tmp_slice[i] = v
				i++
			}
			um.RUnlock()
			//fmt.Println("Sending Values")
			for _, v := range tmp_slice {
				ret_chan <- v
			}
		}
		//fmt.Println("Closing Visit Chan")
		close(ret_chan)
	}()
	return ret_chan
}

func (um *UrlMap) Delete(key Url) {
	um.Lock()
	if um.use_disk {
		um.dkst.Delete(key)
	} else {
		delete(um.mp, key)
	}
	um.Unlock()
}
