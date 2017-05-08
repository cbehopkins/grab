package grab

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type UrlMap struct {
	sync.RWMutex
	disk_lock sync.Mutex
	// something in mp must be on the disk - it is a cache
	// something in mp_tow is not on the disk
	mp       map[Url]struct{} // Doubles up as local read cache when using disk
	mp_tow   map[Url]struct{} // URLs waiting to be written
	use_disk bool
	closed   bool
	dkst     *DkStore
	cachewg  *sync.WaitGroup
}

func NewUrlMap(filename string, overwrite, compact bool) *UrlMap {
	var use_disk bool
	if filename != "" {
		use_disk = true
	}
	itm := new(UrlMap)
	itm.use_disk = use_disk
	itm.mp = make(map[Url]struct{})
	itm.cachewg = new(sync.WaitGroup)
	if use_disk {
		itm.dkst = NewDkStore(filename, overwrite)
		if compact {
			fmt.Println("Compacting Database:", filename)
			// Compact the current store and write to temp file
			itm.dkst.compact("/tmp/compact.gkvlite")
			itm.dkst.Close() // Close before:
			// move temp to current
			err := os.Rename("/tmp/compact.gkvlite", filename)
			check(err)
			// load in the new smaller file
			itm.dkst = NewDkStore(filename, false)
			fmt.Println("Compact Complete")
		}
		itm.mp_tow = make(map[Url]struct{})
		go itm.flusher()
		self_read := false
		if self_read {
			//fmt.Println("loafing:", filename)
			for _ = range itm.dkst.GetStringKeys() {
			}
			fmt.Println("done:", filename)
		}
	}
	return itm
}
func (um *UrlMap) ageCache() {
	// remove items from  the cache
	// remove half of them
	num_to_remove := len(um.mp) >> 1

	i := 0
	for key, _ := range um.mp {
		delete(um.mp, key)
		if i >= num_to_remove {
			return
		} else {
			i++
		}
	}
}


// We have a local version of things to write
// Get that on the disk
func (um *UrlMap) localFlush() {
	for key, _ := range um.mp_tow {
		//fmt.Println("Adding Key:", key)
		um.dkst.SetAny(key, "")
	}
	um.mp_tow = make(map[Url]struct{})
}
func (um *UrlMap) diskFlush() {
	um.dkst.Flush()
}

// Flush from local cache to the disk
func (um *UrlMap) Flush() {
	if um.use_disk {
		um.Lock()
		um.localFlush()
		um.diskFlush()
		um.ageCache()
		um.Unlock()
	}
}

// Sync the disk data to the file system
func (um *UrlMap) Sync() {
	if um.use_disk {
		um.disk_lock.Lock()
		um.dkst.Sync()
		um.disk_lock.Unlock()
	}
}

// Close the disk off
func (um *UrlMap) Close() {
	// Close is a special case that operates on
	// both the collection and the disk itself
	// so we need both locks
	if um.use_disk {
		um.Lock()
		um.localFlush()
		um.disk_lock.Lock()
		um.diskFlush()
		um.dkst.Sync()
		um.dkst.Close()
		um.closed = true
		um.disk_lock.Unlock()
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
		um.RLock() // Only get the lock to read the closed flag
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
		// Check in the local cache first
		_, ok = um.mp[key]
		if !ok {
			_, ok = um.mp_tow[key]

			if !ok {
				ok = um.dkst.Exist(key)
			}
			if ok {
				// if we find it exists anywhere
				// then add it to the cache
				um.cachewg.Add(1)
				go func() {
					// add it to the cache
					um.Lock()
					um.mp[key] = struct{}{}
					um.Unlock()
					um.cachewg.Done()
				}()
			}
		}
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
		//um.dkst.SetAny(key, "")
		um.mp_tow[key] = struct{}{}
		//um.mp[key] = struct{}{}
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
		return um.dkst.Count() + len(um.mp_tow)
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
		return um.dkst.Size() + len(um.mp_tow)
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
		um.Lock()
		um.localFlush()
		um.Unlock()
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
		//um.Lock()
		//um.localFlush()
		//um.Unlock()
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
func (um *UrlMap) FlushWrites() {
	um.Lock()
	um.localFlush()   // Ensure the write cache is up to date
	um.Unlock()
	//um.cachewg.Wait() // Ensure the read cache is up to date
}

// Similar to Visit() but we supply the map of references
// This attempts to search through the database
// finding a good selection of URLs
func (um *UrlMap) VisitMissing(refr *TokenChan) map[Url]struct{} {
	ret_map := make(map[Url]struct{})
	//um.FlushWrites()  // Flush writes will happen later with go um.Flush()

	um.RLock()
	if um.use_disk {
		if um.closed {
			um.RUnlock()
			return ret_map
		} else {
			// Get up to 100 things that aren't on the TokenChan
			ret_map_url := um.dkst.GetMissing(10000, refr)
			um.RUnlock()
			for key, value := range ret_map_url {
				ret_map[NewUrl(key)] = value
			}
			// We've done reading, so write out the cache
			// while the disk is not busy
			go um.Flush()
			return ret_map
		}
	} else {
		for v, _ := range um.mp {
			ret_map[v] = struct{}{}
		}
		um.RUnlock()
		// We've finsihed reading - excellent time
		// to do lots of writes
		go um.Flush()
	}
	return ret_map
}

func (um *UrlMap) Delete(key Url) {
	um.cachewg.Wait()
	um.Lock()
	if um.use_disk {
		if !um.closed {
			var ok bool
			_, ok = um.mp_tow[key]
			if ok {
				delete(um.mp_tow, key)
				_, ok = um.mp[key]
				if ok {
					delete(um.mp, key)
				}

			} else {
				// It may or may not exist in the cache
				um.dkst.Delete(key)
				_, ok = um.mp[key]
				if ok {
					delete(um.mp, key)
				}
			}
		}
	} else {
		delete(um.mp, key)
	}
	um.Unlock()
}

func (um *UrlMap) PrintWorkload() {
	um.Lock()
	um.localFlush()
	um.Unlock()
	um.RLock()
	um.dkst.PrintWorkload()
	um.RUnlock()
}
