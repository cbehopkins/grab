package grab

import (
	//"fmt"
	"sync"
	"time"
)

type UrlMap struct {
	sync.RWMutex
	disk_lock sync.Mutex // Temporaraly not using this as could be causing corruption
	mp        map[Url]struct{}
	use_disk  bool
	closed    bool
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

// Flush from local cache to the disk
func (um *UrlMap) Flush() {
	if um.use_disk {
		um.Lock()
		um.dkst.Flush()
		um.Unlock()
	}
}

// Sync the disk data to the file system
func (um *UrlMap) Sync() {
	if um.use_disk {
		um.Lock()
		um.dkst.Sync()
		um.Unlock()
	}
}

// Close the disk off
func (um *UrlMap) Close() {
	// Close is a special case that operates on
	// both the collection and the disk itself
	// so we need both locks
	if um.use_disk {
		um.Lock()
		um.dkst.Flush()
		um.dkst.Sync()
		um.dkst.Close()
		um.closed = true
		um.Unlock()
	}
}

// Occasionally flush and sync
// keeps memory down?
// Means that if things crash we don't lose everything
func (um *UrlMap) flusher() {
	for {
		time.Sleep(100 * time.Second)
		// we don't bother to get a single lock here for both operations as
		// it's fine to let other things sneak in between these potentially
		// long operations
		um.RLock()
		if !um.closed {
			um.RUnlock()
			um.Flush()
			um.Sync()
		} else {
			um.RUnlock()
			return
		}
	}
}
func (um *UrlMap) Exist(key Url) bool {
	//fmt.Println("Exist lock for:", key)
	um.RLock()
	var ok bool
	if um.use_disk {
		ok = um.dkst.Exist(key)
	} else {
		_, ok = um.mp[key]
	}
	um.RUnlock()
	//fmt.Println("Exist Returned for:", key)
	return ok
}
func (um *UrlMap) Set(key Url) {
	//fmt.Println("Get lock for:", key)
	um.Lock()
	//fmt.Println("Got Lock")
	if um.use_disk {
		um.dkst.SetAny(key, "")
	} else {
		um.mp[key] = struct{}{}
	}
	um.Unlock()
	//fmt.Println("Lock Returned for:", key)
}

// Check is a useful test function
// Allows you to check if something is in the
// map by visiting every one manually
// rather than doing an actual lookup.
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

// Count is what you should be using to get an
// accurate count of the number of items
func (um *UrlMap) Count() int {
	um.RLock()
	defer um.RUnlock()
	if um.use_disk {
		if um.closed {
			return 0
		}
		return um.dkst.Count()
	}
	return len(um.mp)
}

// Size provides a backwards compatable interface.
// For performancs it returns 1 if Count()>0
// This saves trawling the whole tree
func (um *UrlMap) Size() int {
	um.RLock()
	defer um.RUnlock()
	if um.use_disk {
		if um.closed {
			return 0
		}
		return um.dkst.Size()
	}
	return len(um.mp)
}

// Return a channel that we can read the (current) list of Urls From
// This will lock any updates to the database until we have read them all
// If this is a problem use Visit() which reads a limited number into
// a buffer first
func (um *UrlMap) VisitAll() chan Url {
	ret_chan := make(chan Url)
	//fmt.Println("Called Visit")

	go func() {
		//fmt.Println("Grabbing Lock")
		um.RLock()
		//fmt.Println("Got Lock")
		if um.use_disk {
			if !um.closed {
				for v := range um.dkst.GetStringKeys() {
					//fmt.Println("Visit Url:", v)
					ret_chan <- NewUrl(v)
				}
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

// Visit (some of) the Urls
// Return a channel to read them from
func (um *UrlMap) Visit() chan Url {
	ret_chan := make(chan Url)
	//fmt.Println("Called Visit")

	go func() {
		//fmt.Println("Grabbing Lock")
		um.RLock()
		//fmt.Println("Got Lock")
		if um.use_disk {
			if um.closed {
				um.RUnlock()
				close(ret_chan)
				return
			} else {
				string_array := um.dkst.GetStringKeysArray(100)
				um.RUnlock()
				for _, v := range string_array {
					//fmt.Println("Visit Url:", v)
					ret_chan <- NewUrl(v)
				}
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

// Similar to Visit() but we supply the map of references
// This attempts to search through the database
// finding a good selection of URLs
func (um *UrlMap) VisitMissing(refr *TokenChan) chan Url {
	ret_chan := make(chan Url)
	go func() {
		um.RLock()
		if um.use_disk {
			if um.closed {
				um.RUnlock()
				close(ret_chan)
				return
			} else {
				// Get up to 100 things that aren't on the TokenChan
				string_array := um.dkst.GetMissing(10000, refr)
				um.RUnlock()
				for _, v := range string_array {
					//fmt.Println("Visit Url:", v)
					ret_chan <- NewUrl(v)
				}
			}
		} else {
			tmp_slice := make([]Url, len(um.mp))
			i := 0
			for v, _ := range um.mp {
				tmp_slice[i] = v
				i++
			}
			um.RUnlock()
			for _, v := range tmp_slice {
				ret_chan <- v
			}
		}
		close(ret_chan)
	}()
	return ret_chan
}

func (um *UrlMap) Delete(key Url) {
	um.Lock()
	if um.use_disk {
		if !um.closed {
			um.dkst.Delete(key)
		}
	} else {
		delete(um.mp, key)
	}
	um.Unlock()
}
