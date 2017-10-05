package grab

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type UrlMap struct {
	sync.RWMutex
	disk_lock sync.Mutex
	// something in mp must be on the disk - it is a cache
	// something in mp_tow is not on the disk
	mp            map[string]Url // Doubles up as local read cache when using disk
	mp_tow        map[string]Url // URLs waiting to be written
	closed        bool
	dkst          *DkStore
	cachewg       *sync.WaitGroup
	UseReadCache  bool
	UseWriteCache bool
}

// CopyFile copies a file from src to dst. If src and dst files exist, and are
// the same, then return success. Otherise, attempt to create a hard link
// between the two files. If that fail, copy the file contents from src to dst.
func CopyFile(src, dst string) (err error) {
	sfi, err := os.Stat(src)
	if err != nil {
		return
	}
	if !sfi.Mode().IsRegular() {
		// cannot copy non-regular files (e.g., directories,
		// symlinks, devices, etc.)
		return fmt.Errorf("CopyFile: non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
	}
	dfi, err := os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("CopyFile: non-regular destination file %s (%q)", dfi.Name(), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			return
		}
	}
	if err = os.Link(src, dst); err == nil {
		return
	}
	err = copyFileContents(src, dst)
	return
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}

func NewUrlMap(filename string, overwrite, compact bool) *UrlMap {
	itm := new(UrlMap)
	itm.mp = make(map[string]Url)
	itm.dkst = NewDkStore(filename, overwrite)
	if compact {
		fmt.Println("Compacting Database:", filename)
		// Compact the current store and write to temp file
		itm.dkst.compact(os.TempDir() + "/compact.gkvlite")
		itm.dkst.Close() // Close before:
		// move temp to current

		//err := os.Rename(os.TempDir()+"/compact.gkvlite", filename)
		err := CopyFile(os.TempDir()+"/compact.gkvlite", filename)
		if err != nil {
			log.Fatalf("Copy problem\nType:%T\nVal:%v\n", err, err)
		}
		err = os.Remove(os.TempDir() + "/compact.gkvlite")
		check(err)
		// load in the new smaller file
		itm.dkst = NewDkStore(filename, false)
		fmt.Println("Compact Complete")
	}
	go itm.flusher()

	// Self read is a debug function
	// when we suspect problems loading the array
	self_read := false
	if self_read {
		//fmt.Println("loafing:", filename)
		for range itm.dkst.GetAnyKeys() {
		}
		fmt.Println("done:", filename)
	}

	return itm
}
func (um *UrlMap) ageCache() {
	// remove items from  the cache
	// remove half of them
	num_to_remove := len(um.mp) >> 1

	i := 0
	for key := range um.mp {
		delete(um.mp, key)
		if i >= num_to_remove {
			return
		} else {
			i++
		}
	}
}
func (um *UrlMap) SetReadCache() {
	um.UseReadCache = true
	if um.UseReadCache {
		um.cachewg = new(sync.WaitGroup)
	}
}
func (um *UrlMap) SetWriteCache() {
	um.UseWriteCache = true
	um.mp_tow = make(map[string]Url)

}

// We have a local version of things to write
// Get that on the disk
func (um *UrlMap) localFlush() {
	if um.UseWriteCache {
		for _, val := range um.mp_tow {
			//fmt.Println("Adding Key:", key)
			//um.dkst.SetAny(key, "")
			um.dkst.SetUrl(val)
		}
		um.mp_tow = make(map[string]Url)
	}
}
func (um *UrlMap) diskFlush() {
	um.dkst.Flush()
}

// Flush from local cache to the disk
func (um *UrlMap) Flush() {
	um.Lock()
	um.localFlush() // Local write cache
	um.diskFlush()  // The store itself
	um.ageCache()   // The read cache
	um.Unlock()
}

// Sync the disk data to the file system
func (um *UrlMap) Sync() {
	um.disk_lock.Lock()
	um.dkst.Sync()
	um.disk_lock.Unlock()
}

// Close the disk off
func (um *UrlMap) Close() {
	// Close is a special case that operates on
	// both the collection and the disk itself
	// so we need both locks
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

func (um *UrlMap) Properties(key_u Url) (promiscuous, shallow, exist bool) {
	exist = um.Exist(key_u)
	if exist {
		key := key_u.Key()
		tmp_url := um.GetUrl(key)
		promiscuous = tmp_url.GetPromiscuous()
		shallow = tmp_url.GetShallow()
	}
	return
}

func (um *UrlMap) Exist(key_u Url) bool {
	if false {
		key := key_u.Key()
		return um.ExistS(key)
	} else {
		key_ba := key_u.ToBa()
		return um.ExistS(string(key_ba))
	}
}
func (um *UrlMap) ExistS(key string) bool {
	//fmt.Println("Exist lock for:", key)
	um.RLock()
	var ok bool
	// Check in the local cache first
	if um.UseReadCache {
		_, ok = um.mp[key]
	}

	if um.UseWriteCache && !ok {
		_, ok = um.mp_tow[key]
	}
	if !ok {
		ok = um.dkst.Exist(key)
	}

	if ok && um.UseReadCache {
		// if we find it exists anywhere
		// then add it to the cache
		um.cachewg.Add(1)
		go func() {
			// add it to the cache
			um.Lock()
			um.mp[key] = um.GetUrl(key)
			um.Unlock()
			um.cachewg.Done()
		}()
	}
	um.RUnlock()

	//fmt.Println("Exist Returned for:", key)
	return ok
}
func (um *UrlMap) GetUrl(key string) Url {
	// TBD change this to actually do the lookup
	if true {
		var itm Url
		var ok bool
		if um.UseReadCache {
			itm, ok = um.mp[key]
			if ok {
				return itm
			}
		}
		if um.UseWriteCache && !ok {
			itm, ok = um.mp_tow[key]
			if ok {
				return itm
			}
		}
		itm = um.dkst.GetUrl(key)
		itm.Initialise()
		return itm

	} else {
		// Not looking up, just return a new one
		tmp := NewUrl(key)
		tmp.Base()
		return tmp
	}
}

//func (um *UrlMap) Set(key_u Url) {
//	um.SetBa(key_u.ToBa())
//}
func (um *UrlMap) Set(key_u Url) {
	key_ba := key_u.ToBa()
	key_string := string(key_ba)
	//fmt.Println("Get lock for:", key)
	um.Lock()
	//fmt.Println("Got Lock")
	if um.UseWriteCache {
		um.mp_tow[key_string] = key_u
	} else {

		//um.dkst.SetAny(key_ba, "")
		fmt.Println("Storing URL:", key_u)
		um.dkst.SetUrl(key_u)
	}
	um.Unlock()
	//fmt.Println("Lock Returned for:", key)
}

//func (um *UrlMap) SetS(key string) {
//	//fmt.Println("Get lock for:", key)
//	um.Lock()
//	//fmt.Println("Got Lock")
//	if um.UseWriteCache {
//    log.Fatal("Not Supported")
//		//.mp_tow[key] =
//	} else {
//		um.dkst.SetAny(key, "")
//	}
//	um.Unlock()
//	//fmt.Println("Lock Returned for:", key)
//}

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
	if um.closed {
		return 0
	}
	size := um.dkst.Count()
	if um.UseWriteCache {
		size += len(um.mp_tow)
	}
	return size
}

// Size provides a backwards compatable interface.
// For performancs it returns 1 if Count()>0
// This saves trawling the whole tree
func (um *UrlMap) Size() int {
	um.RLock()
	defer um.RUnlock()
	if um.closed {
		return 0
	}
	size := um.dkst.Size()
	if um.UseWriteCache {
		size += len(um.mp_tow)
	}
	return size
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
		if !um.closed {
			for ba := range um.dkst.GetAnyKeys() {
				//fmt.Println("Visit Url:", v)
				ret_chan <- um.dkst.UrlFromBa(ba)
			}
		}
		um.RUnlock()

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
		if um.closed {
			um.RUnlock()
			close(ret_chan)
			return
		} else {
			string_array := um.dkst.GetAnyKeysArray(100)
			um.RUnlock()
			for _, ba := range string_array {
				//fmt.Println("Visit Url:", v)
				ret_chan <- um.dkst.UrlFromBa(ba)
			}
		}
		//fmt.Println("Closing Visit Chan")
		close(ret_chan)
	}()
	return ret_chan
}
func (um *UrlMap) FlushWrites() {
	um.Lock()
	um.localFlush() // Ensure the write cache is up to date
	um.Unlock()
	//um.cachewg.Wait() // Ensure the read cache is up to date
}

// Similar to Visit() but we supply the map of references
// This attempts to search through the database
// finding a good selection of URLs
func (um *UrlMap) VisitMissing(refr *TokenChan) map[string]struct{} {
	ret_map := make(map[string]struct{})
	um.FlushWrites() // Flush writes will happen later with go um.Flush()

	um.RLock()
	if um.closed {
		um.RUnlock()
		return ret_map
	} else {
		// Get up to 100 things that aren't on the TokenChan
		ret_map_url := um.dkst.GetMissing(10000, refr)
		um.RUnlock()
		s := Spinner{scaler: 100}
		cnt := 0
		for key, value := range ret_map_url {
			if false {
				cnt++
				s.PrintSpin(cnt)
			}
			ret_map[key] = value
		}
		// We've done reading, so write out the cache
		// while the disk is not busy
		go um.Flush()
		return ret_map
	}
	return ret_map
}
func (um *UrlMap) Delete(key_u Url) {
	key := key_u.Key()
	um.DeleteS(key)
}
func (um *UrlMap) DeleteS(key string) {
	if um.UseReadCache {
		um.cachewg.Wait()
	}
	um.Lock()
	if !um.closed {
		var ok bool
		if um.UseWriteCache {
			_, ok = um.mp_tow[key]
			if ok {
				delete(um.mp_tow, key)
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

func (um *UrlMap) PrintWorkload() {
	um.Lock()
	um.localFlush()
	um.Unlock()
	um.RLock()
	um.dkst.PrintWorkload()
	um.RUnlock()
}
