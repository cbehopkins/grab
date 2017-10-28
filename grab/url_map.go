package grab

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// URLMap represents the prototype of a map from url string(key)  to the struct
// Used to track each of visited and unvisited urls
type URLMap struct {
	sync.RWMutex
	diskLock sync.Mutex
	// something in mp must be on the disk - it is a cache
	// something in mp_tow is not on the disk
	mp            map[string]URL // Local Read Cache
	mpTow         map[string]URL // Local Write Cache
	closed        bool
	dkst          *DkCollection
	cachewg       *sync.WaitGroup
	UseReadCache  bool
	UseWriteCache bool
}

// NewURLMap creates a new map
func NewURLMap(filename string, overwrite, compact bool) *URLMap {
	itm := new(URLMap)
	itm.mp = make(map[string]URL)
	itm.dkst = NewDkCollection(filename, overwrite)
	if compact {
		fmt.Println("Compacting Database:", filename)
		// Compact the current store and write to temp file
		compactFilename := os.TempDir() + "/compact.gkvlite"
		itm.dkst.ds.compact(compactFilename)
		itm.dkst.ds.Close() // Close before:
		// move temp to current

		err := MoveFile(compactFilename, filename)
		if err != nil {
			log.Fatalf("Move problem\nType:%T\nVal:%v\n", err, err)
		}

		// load in the new smaller file
		itm.dkst = NewDkCollection(filename, false)
		fmt.Println("Compact Complete")
	}
	go itm.flusher()

	// Self read is a debug function
	// when we suspect problems loading the array
	selfRead := false
	if selfRead {
		for range itm.dkst.GetAnyKeys() {
		}
		fmt.Println("done:", filename)
	}

	return itm
}
func (um *URLMap) ageCache() {
	// remove items from  the cache
	// remove half of them
	numToRemove := len(um.mp) >> 1

	i := 0
	for key := range um.mp {
		delete(um.mp, key)
		if i >= numToRemove {
			return
		}
		i++
	}
}

// SetReadCache activates the read cacheing
func (um *URLMap) SetReadCache() {
	um.UseReadCache = true
	if um.UseReadCache {
		um.cachewg = new(sync.WaitGroup)
	}
}

// SetWriteCache activates the write caching
func (um *URLMap) SetWriteCache() {
	um.UseWriteCache = true
	um.mpTow = make(map[string]URL)

}

// We have a local version of things to write
// Get that on the disk
func (um *URLMap) localFlush() {
	if um.UseWriteCache {
		for _, val := range um.mpTow {
			//fmt.Println("Adding Key:", key)
			//um.dkst.SetAny(key, "")
			um.dkst.SetURL(val)
		}
		um.mpTow = make(map[string]URL)
	}
}
func (um *URLMap) diskFlush() {
	um.dkst.ds.Flush()
}

// Flush from local cache to the disk
func (um *URLMap) Flush() {
	um.Lock()
	um.localFlush() // Local write cache
	um.diskFlush()  // The store itself
	um.ageCache()   // The read cache
	um.Unlock()
}

// Sync the disk data to the file system
func (um *URLMap) Sync() {
	um.diskLock.Lock()
	um.dkst.ds.Sync()
	um.diskLock.Unlock()
}

// Close the disk off
func (um *URLMap) Close() {
	// Close is a special case that operates on
	// both the collection and the disk itself
	// so we need both locks
	um.Lock()
	um.localFlush()
	um.diskLock.Lock()
	um.diskFlush()
	um.dkst.ds.Sync()
	um.dkst.ds.Close()
	um.closed = true
	um.diskLock.Unlock()
	um.Unlock()
}

// Occasionally flush and sync
// keeps memory down?
// Means that if things crash we don't lose everything
func (um *URLMap) flusher() {
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

// Properties of the url
func (um *URLMap) Properties(keyU URL) (promiscuous, shallow, exist bool) {
	exist = um.Exist(keyU)
	if exist {
		key := keyU.Key()
		tmpURL := um.GetURL(key)
		promiscuous = tmpURL.GetPromiscuous()
		shallow = tmpURL.GetShallow()
	}
	return
}

// Exist returns true if the url exists in the map
func (um *URLMap) Exist(keyU URL) bool {
	if true {
		key := keyU.Key()
		return um.ExistS(key)
	}
	keyBa := keyU.ToBa()
	return um.ExistS(string(keyBa))
}

// ExistS as Exist but give it a string
func (um *URLMap) ExistS(key string) bool {
	//fmt.Println("Exist lock for:", key)
	um.RLock()
	var ok bool
	// Check in the local cache first
	if um.UseReadCache {
		_, ok = um.mp[key]
	}

	if um.UseWriteCache && !ok {
		_, ok = um.mpTow[key]
	}
	var dkOk bool
	if !ok {
		dkOk = um.dkst.Exist(key)
	}

	if !ok && dkOk && um.UseReadCache {
		// if we find it exists anywhere
		// then add it to the cache
		um.cachewg.Add(1)
		go func() {
			// add it to the cache
			um.Lock()
			um.mp[key] = um.getURLDisk(key)
			um.Unlock()
			um.cachewg.Done()
		}()
	}
	um.RUnlock()
	ok = ok || dkOk
	//fmt.Println("Exist Returned for:", key)
	return ok
}

// getURLDisk explicitly gets it from the disk
func (um *URLMap) getURLDisk(key string) URL {
	itm := um.dkst.GetURL(key)
	//itm.Initialise()
	return itm
}

// GetURL from wherever it lies
func (um *URLMap) GetURL(key string) URL {
	if true {
		var itm URL
		var ok bool
		um.RLock()
		defer um.RUnlock()
		if um.UseReadCache {
			itm, ok = um.mp[key]
			if ok {
				//um.RUnlock()
				return itm
			}
		}
		if um.UseWriteCache && !ok {
			itm, ok = um.mpTow[key]
			if ok {
				//um.RUnlock()
				return itm
			}
		}
		//um.RUnlock()
		itm = um.getURLDisk(key)
		return itm

	}
	// Not looking up, just return a new one
	tmp := NewURL(key)
	tmp.Base()
	return tmp

}

// Set the url as one of interest
func (um *URLMap) Set(keyU URL) {
	keyBa := keyU.ToBa()
	keyString := string(keyBa)
	//fmt.Println("Get lock for:", key)
	um.Lock()
	//fmt.Println("Got Lock")
	if um.UseWriteCache {
		um.mpTow[keyString] = keyU
	} else {
		//um.dkst.SetAny(key_ba, "")
		//fmt.Println("Storing URL:", key_u)
		um.dkst.SetURL(keyU)
	}
	um.Unlock()
	//fmt.Println("Lock Returned for:", key)
}

// Check is a useful test function
// Allows you to check if something is in the
// map by visiting every one manually
// rather than doing an actual lookup.
func (um *URLMap) Check(key URL) bool {
	// Be 100% sure things work as we expect!
	srcChan := um.VisitAll(nil)
	found := false
	for v := range srcChan {
		if v == key {
			found = true
		}
	}
	return found
}

// Count is what you should be using to get an
// accurate count of the number of items
func (um *URLMap) Count() int {
	um.RLock()
	defer um.RUnlock()
	if um.closed {
		return 0
	}
	size := um.dkst.Count()
	if um.UseWriteCache {
		size += len(um.mpTow)
	}
	return size
}

// Size provides a backwards compatable interface.
// For performancs it returns 1 if Count()>0
// This saves trawling the whole tree
func (um *URLMap) Size() int {
	um.RLock()
	defer um.RUnlock()
	if um.closed {
		return 0
	}
	size := um.dkst.Size()
	if um.UseWriteCache {
		size += len(um.mpTow)
	}
	return size
}

// VisitAll will return a channel that we can read the (current) list of Urls From
// This will lock any updates to the database until we have read them all
// If this is a problem use Visit() which reads a limited number into
// a buffer first
func (um *URLMap) VisitAll(closeChan chan struct{}) chan URL {
	retChan := make(chan URL)
	//fmt.Println("Called Visit")

	go func() {
		//fmt.Println("Grabbing Lock")
		um.Lock()
		um.localFlush()
		um.Unlock()
		um.RLock()
		//fmt.Println("Got Lock")
		tmpChan := um.dkst.GetAnyKeys()
		var finished bool
		for !um.closed && !finished {
			select {
			case _, ok := <-closeChan:
				if !ok {
					//fmt.Println("closeChan has requested we abort visitAll")
					finished = true
				} else {
					log.Fatal("Odd! Data on channel closer is not expected")
				}
			case ba, ok := <-tmpChan:
				//fmt.Println("Visit URL:", v)
				if ok {
					retChan <- um.dkst.URLFromBa(ba)
				} else {
					//fmt.Println("Run out of urls to visit")
					finished = true
				}
			}
		}
		um.RUnlock()
		close(retChan)
	}()
	return retChan
}

// Visit (some of) the Urls
// Return a channel to read them from
func (um *URLMap) Visit() chan URL {
	retChan := make(chan URL)
	//fmt.Println("Called Visit")

	go func() {
		//fmt.Println("Grabbing Lock")
		//um.Lock()
		//um.localFlush()
		//um.Unlock()
		um.RLock()
		//fmt.Println("Got Lock")
		if um.closed {
			um.RUnlock()
			close(retChan)
			return
		}
		stringArray := um.dkst.GetAnyKeysArray(100)
		um.RUnlock()
		for _, ba := range stringArray {
			retChan <- um.dkst.URLFromBa(ba)
		}
		//fmt.Println("Closing Visit Chan")
		close(retChan)
	}()
	return retChan
}

// VisitFrom start a visit but from a specified start location
func (um *URLMap) VisitFrom(startURL URL) chan URL {
	retChan := make(chan URL)
	//fmt.Println("Called Visit")

	go func() {
		//fmt.Println("Grabbing Lock")
		//um.Lock()
		//um.localFlush()
		//um.Unlock()
		um.RLock()
		//fmt.Println("Got Lock")
		if um.closed {
			um.RUnlock()
			close(retChan)
			return
		}
		stringArray := um.dkst.GetAnyKeysArrayFrom(100, startURL.ToBa())
		um.RUnlock()
		for _, ba := range stringArray {
			retChan <- um.dkst.URLFromBa(ba)
		}
		//fmt.Println("Closing Visit Chan")
		close(retChan)
	}()
	return retChan
}

// flushWrites as it says
func (um *URLMap) flushWrites() {
	um.Lock()
	um.localFlush() // Ensure the write cache is up to date
	um.Unlock()
	//um.cachewg.wait() // Ensure the read cache is up to date
}

// VisitMissing is similar to Visit() but we supply the map of references
// This attempts to search through the database
// finding a good selection of URLs
func (um *URLMap) VisitMissing(refr *TokenChan) map[string]struct{} {
	retMap := make(map[string]struct{})
	um.flushWrites() // Flush writes will happen later with go um.Flush()

	um.RLock()
	if um.closed {
		um.RUnlock()
	} else {
		//fmt.Println("Getting 10000 items")
		// Get up to 100 things that aren't on the TokenChan
		retMapURL := um.dkst.GetMissing(10000, refr)
		//fmt.Println("Got 1000 items")
		um.RUnlock()
		s := Spinner{scaler: 100}
		cnt := 0
		for key, value := range retMapURL {
			if false {
				cnt++
				s.PrintSpin(cnt)
			}
			retMap[key] = value
		}
		// We've done reading, so write out the cache
		// while the disk is not busy
		go um.Flush()
	}
	return retMap
}

// Delete the specified item from the map
func (um *URLMap) Delete(keyU URL) {
	key := keyU.Key()
	um.DeleteS(key)
}

// DeleteS as delete, but specify as a string
func (um *URLMap) DeleteS(key string) {
	if um.UseReadCache {
		um.cachewg.Wait()
	}
	um.Lock()
	if !um.closed {
		var ok bool
		if um.UseWriteCache {
			_, ok = um.mpTow[key]
			if ok {
				delete(um.mpTow, key)
			}
		}
		if um.UseReadCache {
			_, ok = um.mp[key]
			if ok {
				delete(um.mp, key)
			}
		}

		// It may or may not exist in the cache
		um.dkst.Delete(key)
	}
	um.Unlock()
}

// PrintWorkload usage statistics
func (um *URLMap) PrintWorkload() {
	um.Lock()
	um.localFlush()
	um.Unlock()
	um.RLock()
	um.dkst.PrintWorkload()
	um.RUnlock()
}

// LockTest tests that we can lock and unlock correctly
func (um *URLMap) LockTest() {
	um.RLock()
	um.RUnlock()
}
