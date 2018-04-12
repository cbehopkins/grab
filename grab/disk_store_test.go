package grab

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

const useTestParallel = true
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var randLock sync.Mutex
var src = rand.NewSource(time.Now().UnixNano())

func RandStringBytesMaskImprSrc(n int) string {
	b := make([]byte, n)
	randLock.Lock()
	defer randLock.Unlock()
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}
func (dkst *URLMap) checkStore(backupHash map[string]struct{}, numEntries, maxStrLen int) {
	dkst.checkStoreD(backupHash, numEntries, maxStrLen, 0, true)
}
func (dkst *URLMap) checkStoreD(backupHash map[string]struct{}, numEntries, maxStrLen int, delay time.Duration, testDiskFull bool) {
	//dkst.localFlush()
	if false {
		for key := range dkst.dkst.GetAnyKeys() {
			log.Println("Disk contains key:", string(key))
			if !dkst.ExistS(string(key)) {
				log.Fatal("self check fail 0")
			}
		}
		for v := range dkst.VisitAll(nil) {
			key := v.Key()
			//log.Println("Disk contains key:", key)
			if !dkst.ExistS(key) {
				log.Fatal("self check fail 1")
			}
		}
	}
	for v := range backupHash {
		key := NewURL(v)
		//log.Println("Checking for key", key)
		if !dkst.Exist(key) {
			log.Println(dkst.mp)
			log.Fatal("Error, missing key from URLmap disk", v)
		}
		time.Sleep(delay)
	}
	if testDiskFull {
		for v := range dkst.VisitAll(nil) {
			_, ok := backupHash[v.URL()]
			if !ok {
				log.Fatal("Error, extra key in disk", v)
			}
			time.Sleep(delay)
		}
	}
	// Try some random entries and see if there is one in one but not the other
	// Yes I know this should never happen. That's the point.
	// (next I will write a test for 1!=2)
	for i := 0; i < numEntries; i++ {
		strLen := rand.Int31n(int32(maxStrLen)) + 1
		tstString := RandStringBytesMaskImprSrc(int(strLen))
		_, ok := backupHash[tstString]
		if !ok {
			// make sure if doesn't exist in backup_hash
			// then it doesn't in the store
			if dkst.Exist(NewURL(tstString)) {
				log.Fatalf("%s is in dkst, but not in bakup\n", tstString)
			}
		} else {
			// try again
			i--
		}
		time.Sleep(delay)
	}
}

func (dc *DkCollection) checkStore(backupHash map[string]struct{}, numEntries, maxStrLen int) {

	for v := range backupHash {
		if !dc.Exist(NewURL(v).ToBa()) {
			log.Fatal("Error, missing key from disk", v)
		}
	}
	for ba := range dc.GetAnyKeys() {
		ur := dc.URLFromBa(ba)
		v := ur.String()
		_, ok := backupHash[v]
		if !ok {
			log.Fatal("Error, extra key in disk", v)
		}
	}
	// Try some random entries and see if there is one in one but not the other
	// Yes I know this should never happen. That's the point.
	// (next I will write a test for 1!=2)
	for i := 0; i < numEntries; i++ {
		strLen := rand.Int31n(int32(maxStrLen)) + 1
		tstString := RandStringBytesMaskImprSrc(int(strLen))
		_, ok := backupHash[tstString]
		if !ok {
			// make sure if doesn't exist in backup_hash
			// then it doesn't in the store
			if dc.Exist(tstString) {
				log.Fatalf("%s is in dc, but not in bakup\n", tstString)
			}
		} else {
			// try again
			i--
		}
	}
}

func TestDiskPersistX(t *testing.T) {
	maxStrLen := 256
	numEntriesArray := []int{
		10,
		100, 1000,
		//100000
	}
	wcArray := []bool{true, false}
	rcArray := []bool{true, false}
	for _, numEntries := range numEntriesArray {
		if testing.Short() && numEntries > 1000 {
			t.Skip()
		} else {
			for _, rcl := range rcArray {
				for _, wcl := range wcArray {
					rc := rcl
					wc := wcl
					tString := fmt.Sprintf("num_entries=%d", numEntries)
					ne := numEntries // capture range variable
					if rc {
						tString += ",Read Cached"
					}
					if wc {
						tString += ",Write Cached"
					}
					tFunc := func(t *testing.T) {
						testFilename := tempfilename("", true)
						compactFilename := tempfilename("", true)
						t.Parallel()
						generalDiskPersist(testFilename, compactFilename, maxStrLen, ne, rc, wc)
						check(os.Remove(testFilename))
						check(os.Remove(compactFilename))
					}
					t.Run(tString, tFunc)
				}
			}
		}
	}
}
func BenchmarkDisk(b *testing.B) {
	maxStrLen := 256
	wcArray := []bool{true, false}
	rcArray := []bool{true, false}
	numEntries := 1000
	UseConcSafe = true
	bmd(b, maxStrLen, numEntries, wcArray, rcArray)
	//	UseConcSafe = true
	//	bmd(b, max_str_len, num_entries, wc_array, rc_array)
}
func bmd(b *testing.B, maxStrLen, numEntries int, wcArray, rcArray []bool) {
	for _, rcl := range rcArray {
		for _, wcl := range wcArray {
			rc := rcl
			wc := wcl
			tString := fmt.Sprintf("num_entries=%d", numEntries)
			if rc {
				tString += ",Read Cached"
			}
			if wc {
				tString += ",Write Cached"
			}
			if UseConcSafe {
				tString += ",ConcSafe"
			}
			tFunc := func(b *testing.B) {
				benchmarkDiskPersist(b, maxStrLen, numEntries, rc, wc)
			}
			b.Run(tString, tFunc)
		}
	}

}
func benchmarkDiskPersist(b *testing.B, maxStrLen, numEntries int, useRc, useWc bool) {
	for i := 0; i < b.N; i++ {
		generalDiskAccess(maxStrLen, numEntries, useRc, useWc)
	}
}

// TBD implement use_rc, use_wc checking
func generalDiskAccess(maxStrLen, numEntries int, useRc, useWc bool) {
	testFilename := tempfilename("", false)
	defer rmFilename(testFilename)

	dkst := NewURLMap(testFilename, true, false)
	backupHash0 := make(map[string]struct{})
	backupHash1 := make(map[string]struct{})
	// Create some random entries of varing lengths
	for i := 0; i < numEntries; i++ {
		strLen := rand.Int31n(int32(maxStrLen)) + 1
		tstString := RandStringBytesMaskImprSrc(int(strLen))
		dkst.Set(NewURL(tstString))
		backupHash0[tstString] = struct{}{}
		backupHash1[tstString] = struct{}{}
	}
	dkst.localFlush()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		dkst.checkStoreD(backupHash0, numEntries, maxStrLen, time.Millisecond, false)
		wg.Done()
	}()
	// Create some more that are in the cache
	for i := 0; i < numEntries; i++ {
		strLen := rand.Int31n(int32(maxStrLen)) + 1
		tstString := RandStringBytesMaskImprSrc(int(strLen))
		dkst.Set(NewURL(tstString))
		backupHash1[tstString] = struct{}{}
		time.Sleep(time.Millisecond)
	}
	dkst.checkStore(backupHash1, numEntries, maxStrLen)
	wg.Wait()
}
func generalDiskPersist(testFilename, compactFilename string, maxStrLen, numEntries int, useRc, useWc bool) {
	dkst := NewURLMap(testFilename, true, false)
	backupHash := make(map[string]struct{})

	// Create some random entries of varing lengths
	for i := 0; i < numEntries; i++ {
		strLen := rand.Int31n(int32(maxStrLen)) + 1
		tstString := RandStringBytesMaskImprSrc(int(strLen))
		dkst.Set(NewURL(tstString))
		backupHash[tstString] = struct{}{}
	}
	dkst.localFlush()
	dkst.checkStore(backupHash, numEntries, maxStrLen)

	// Close it all off, make sure it is on the disk
	dkst.Close()
	log.Printf("Okay well the hash itself was consistent, but is it persistant?")

	// filename, overwrite, compact
	dkst1 := NewURLMap(testFilename, false, false)

	dkst1.Flush()
	dkst1.checkStore(backupHash, numEntries, maxStrLen)
	dkst1.Flush()
	log.Println("1st Reload check complete")
	for i := 0; i < numEntries; i++ {
		strLen := rand.Int31n(int32(maxStrLen)) + 1
		tstString := RandStringBytesMaskImprSrc(int(strLen))
		dkst1.Set(NewURL(tstString))
		backupHash[tstString] = struct{}{}
	}
	dkst1.checkStore(backupHash, numEntries, maxStrLen)
	dkst1.Flush()
	dkst1.Close()

	dkst2 := NewURLMap(testFilename, false, false)
	dkst2.checkStore(backupHash, numEntries, maxStrLen)
	log.Println("2nd Reload check complete")

	dkst2.dkst.ds.compact(compactFilename)

	dkst2.Close()

	dkst3 := NewURLMap(compactFilename, false, false)
	dkst3.checkStore(backupHash, numEntries, maxStrLen)
	log.Printf("3rd reload. Check complete")
	dkst3.Close()
	check(os.Remove(testFilename))
	check(os.Remove(compactFilename))
}

func TestDiskStore0(t *testing.T) {
	testFilename := tempfilename("", true)
	defer rmFilename(testFilename)
	if useTestParallel {
		t.Parallel()
	}
	rmFilename(testFilename)

	dkst := NewDkCollection(testFilename, true)
	dkst.SetAny(23, "Hello")
	dkst.SetAny(25, "Goodbye")
	dkst.ds.Flush()
	log.Println("We say:", dkst.GetAny(23))
	log.Println("They say:", dkst.GetAny(25))

	keyChan := dkst.GetIntKeys()
	for key := range keyChan {
		log.Println("The collection contains:", key)
	}
	dkst.ds.Close()
}

func TestDiskStore1(t *testing.T) {
	testFilename := tempfilename("", true)
	defer rmFilename(testFilename)
	if useTestParallel {
		t.Parallel()
	}
	rmFilename(testFilename)

	dkst := NewDkCollection(testFilename, true)
	dkst.SetAny("Hello1", 23)
	dkst.SetAny("Goodbye1", 25)
	dkst.ds.Flush()
	log.Println("We say:", dkst.GetAny("Hello1"))
	log.Println("They say:", dkst.GetAny("Goodbye1"))

	keyChan := dkst.GetAnyKeys()
	for key := range keyChan {
		log.Println("The collection contains:", string(key))
	}
	dkst.ds.Close()
}

func TestDiskStore2(t *testing.T) {
	testFilename := tempfilename("", true)
	defer rmFilename(testFilename)
	if useTestParallel {
		t.Parallel()
	}
	rmFilename(testFilename)

	dkst := NewDkCollection(testFilename, true)
	defer dkst.ds.Close()

	// Let's pretend to have some urls
	var url0 URL
	var url1 URL
	var url2 URL
	url0 = NewURL("http://here.com")
	url1 = NewURL("http://there.com")
	url2 = NewURL("http://nowhere.com")
	if dkst.Size() > 0 {
		log.Fatal("Bigger than zero")
	}
	dkst.SetURL(url0)
	dkst.SetURL(url1)
	if dkst.Size() == 0 {
		log.Fatal("Size is zero")
	}

	if !dkst.Exist(url0) {
		log.Fatal("0 does not exist")
	}
	if !dkst.Exist(url1) {
		log.Fatal("1 does not exist")
	}
	if dkst.Exist(url2) {
		log.Fatal("2 does exist")
	}
	dkst.ds.Flush()
	keyChan := dkst.GetAnyKeys()
	for key := range keyChan {
		log.Println("The collection contains:", string(key))
	}

	// Delete returns true if suceeds
	ok := dkst.Delete(url0)
	if !ok {
		log.Fatal("Delete failed on url_0")
	}
	ok = dkst.Delete(url1)
	if !ok {
		log.Fatal("Delete failed on url_1")
	}

	if dkst.Size() > 0 {
		log.Fatal("Bigger than zero after delete")
	}
}
func TestDiskStore3(t *testing.T) {
	testFilename := tempfilename("", true)
	defer rmFilename(testFilename)
	if useTestParallel {
		t.Parallel()
	}
	rmFilename(testFilename)

	dkst := NewDkCollection(testFilename, true)
	defer dkst.ds.Close()
	// Let's pretend to have some urls
	var url0 URL
	var url1 URL
	var url2 URL
	url0 = NewURL("http://here.com")
	url0.SetPromiscuous()
	url1 = NewURL("http://there.com")
	url2 = NewURL("http://nowhere.com")
	dkst.SetURL(url0)
	dkst.SetURL(url1)
	dkst.SetURL(url2)
	output, err := url0.goMarshalJSON()
	if err != nil {
		log.Fatal("Marshalling error", err)
	}
	if false {
		log.Println("JSON Output", string(output))
	}
	newURL0 := dkst.GetURL("http://here.com")
	if !newURL0.GetPromiscuous() {
		log.Fatal("We should be promiscuous")
	}
	if newURL0.URL() != url0.URL() {
		log.Fatal("URLs are incirrect", newURL0.URL(), url0.URL())
	}
	newURL1 := dkst.GetURL("http://there.com")
	newURL2 := dkst.GetURL("http://nowhere.com")
	if newURL1.GetPromiscuous() {
		log.Fatal("We should not be promiscuous")
	}
	if newURL2.GetPromiscuous() {
		log.Fatal("We should not be promiscuous")
	}

}

func TestDiskRandom0(t *testing.T) {
	testFilename := tempfilename("", true)
	defer rmFilename(testFilename)
	if useTestParallel {
		t.Parallel()
	}
	rmFilename(testFilename)

	dkst := NewDkCollection(testFilename, true)
	dkst.SetAny(23, "Hello")
	dkst.SetAny(24, "Goodbye")
	dkst.SetAny(25, "Goodbye")
	dkst.SetAny(26, "Goodbye")
	log.Println("We say:", dkst.GetAny(23))
	log.Println("They say:", dkst.GetAny(25))

	keyChan := dkst.GetAnyKeysRand()
	for key := range keyChan {
		log.Println("The collection contains:", string(key))
	}
	dkst.ds.Close()
}
