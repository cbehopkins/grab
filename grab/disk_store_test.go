package grab

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())

func RandStringBytesMaskImprSrc(n int) string {
	b := make([]byte, n)
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
func (dkst *UrlMap) checkStore(backup_hash map[string]struct{}, num_entries, max_str_len int) {
	dkst.checkStoreD(backup_hash, num_entries, max_str_len, 0, true)
}
func (dkst *UrlMap) checkStoreD(backup_hash map[string]struct{}, num_entries, max_str_len int, delay time.Duration, test_disk_full bool) {
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
	for v := range backup_hash {
		key := NewUrl(v)
		//log.Println("Checking for key", key)
		if !dkst.Exist(key) {
			log.Println(dkst.mp)
			log.Fatal("Error, missing key from URLmap disk", v)
		}
		time.Sleep(delay)
	}
	if test_disk_full {
		for v := range dkst.VisitAll(nil) {
			_, ok := backup_hash[v.Url()]
			if !ok {
				log.Fatal("Error, extra key in disk", v)
			}
			time.Sleep(delay)
		}
	}
	// Try some random entries and see if there is one in one but not the other
	// Yes I know this should never happen. That's the point.
	// (next I will write a test for 1!=2)
	for i := 0; i < num_entries; i++ {
		str_len := rand.Int31n(int32(max_str_len)) + 1
		tst_string := RandStringBytesMaskImprSrc(int(str_len))
		_, ok := backup_hash[tst_string]
		if !ok {
			// make sure if doesn't exist in backup_hash
			// then it doesn't in the store
			if dkst.Exist(NewUrl(tst_string)) {
				log.Fatalf("%s is in dkst, but not in bakup\n", tst_string)
			}
		} else {
			// try again
			i--
		}
		time.Sleep(delay)
	}
}

func (dkst *DkCollection) checkStore(backup_hash map[string]struct{}, num_entries, max_str_len int) {

	for v := range backup_hash {
		if !dkst.Exist(NewUrl(v).ToBa()) {
			log.Fatal("Error, missing key from disk", v)
		}
	}
	for ba := range dkst.GetAnyKeys() {
		ur := dkst.UrlFromBa(ba)
		v := ur.String()
		_, ok := backup_hash[v]
		if !ok {
			log.Fatal("Error, extra key in disk", v)
		}
	}
	// Try some random entries and see if there is one in one but not the other
	// Yes I know this should never happen. That's the point.
	// (next I will write a test for 1!=2)
	for i := 0; i < num_entries; i++ {
		str_len := rand.Int31n(int32(max_str_len)) + 1
		tst_string := RandStringBytesMaskImprSrc(int(str_len))
		_, ok := backup_hash[tst_string]
		if !ok {
			// make sure if doesn't exist in backup_hash
			// then it doesn't in the store
			if dkst.Exist(tst_string) {
				log.Fatalf("%s is in dkst, but not in bakup\n", tst_string)
			}
		} else {
			// try again
			i--
		}
	}
}
func tempfilename() string {
	tmpfile, err := ioutil.TempFile("", "gkvlite")
	check(err)
	filename := tmpfile.Name()
	tmpfile.Close()
	os.Remove(filename)
	return filename
}

func TestDiskPersistX(t *testing.T) {
	max_str_len := 256
	num_entries_array := []int{
		10,
		100, 1000,
		//100000
	}
	wc_array := []bool{true, false}
	rc_array := []bool{true, false}
	for _, num_entries := range num_entries_array {
		if testing.Short() && num_entries > 1000 {
			t.Skip()
		} else {
			for _, rcl := range rc_array {
				for _, wcl := range wc_array {
					rc := rcl
					wc := wcl
					t_string := fmt.Sprintf("num_entries=%d", num_entries)
					ne := num_entries // capture range variable
					if rc {
						t_string += ",Read Cached"
					}
					if wc {
						t_string += ",Write Cached"
					}
					t_func := func(t *testing.T) {
						//t.Parallel()
						generalDiskPersist(max_str_len, ne, rc, wc)
					}
					t.Run(t_string, t_func)
				}
			}
		}
	}
}
func BenchmarkDisk(b *testing.B) {
	max_str_len := 256
	wc_array := []bool{true, false}
	rc_array := []bool{true, false}
	num_entries := 1000
	UseConcSafe = true
	bmd(b, max_str_len, num_entries, wc_array, rc_array)
	//	UseConcSafe = true
	//	bmd(b, max_str_len, num_entries, wc_array, rc_array)
}
func bmd(b *testing.B, max_str_len, num_entries int, wc_array, rc_array []bool) {
	for _, rcl := range rc_array {
		for _, wcl := range wc_array {
			rc := rcl
			wc := wcl
			t_string := fmt.Sprintf("num_entries=%d", num_entries)
			if rc {
				t_string += ",Read Cached"
			}
			if wc {
				t_string += ",Write Cached"
			}
			if UseConcSafe {
				t_string += ",ConcSafe"
			}
			t_func := func(b *testing.B) {
				benchmarkDiskPersist(b, max_str_len, num_entries, rc, wc)
			}
			b.Run(t_string, t_func)
		}
	}

}
func benchmarkDiskPersist(b *testing.B, max_str_len, num_entries int, use_rc, use_wc bool) {
	for i := 0; i < b.N; i++ {
		generalDiskAccess(max_str_len, num_entries, use_rc, use_wc)
	}
}
func generalDiskAccess(max_str_len, num_entries int, use_rc, use_wc bool) {
	test_filename := tempfilename()
	dkst := NewUrlMap(test_filename, true, false)
	backup_hash0 := make(map[string]struct{})
	backup_hash1 := make(map[string]struct{})
	// Create some random entries of varing lengths
	for i := 0; i < num_entries; i++ {
		str_len := rand.Int31n(int32(max_str_len)) + 1
		tst_string := RandStringBytesMaskImprSrc(int(str_len))
		dkst.Set(NewUrl(tst_string))
		backup_hash0[tst_string] = struct{}{}
		backup_hash1[tst_string] = struct{}{}
	}
	dkst.localFlush()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		dkst.checkStoreD(backup_hash0, num_entries, max_str_len, time.Millisecond, false)
		wg.Done()
	}()
	// Create some more that are in the cache
	for i := 0; i < num_entries; i++ {
		str_len := rand.Int31n(int32(max_str_len)) + 1
		tst_string := RandStringBytesMaskImprSrc(int(str_len))
		dkst.Set(NewUrl(tst_string))
		backup_hash1[tst_string] = struct{}{}
		time.Sleep(time.Millisecond)
	}
	dkst.checkStore(backup_hash1, num_entries, max_str_len)

	wg.Wait()

}
func generalDiskPersist(max_str_len, num_entries int, use_rc, use_wc bool) {
	test_filename := tempfilename()
	dkst := NewUrlMap(test_filename, true, false)
	backup_hash := make(map[string]struct{})

	// Create some random entries of varing lengths
	for i := 0; i < num_entries; i++ {
		str_len := rand.Int31n(int32(max_str_len)) + 1
		tst_string := RandStringBytesMaskImprSrc(int(str_len))
		dkst.Set(NewUrl(tst_string))
		backup_hash[tst_string] = struct{}{}
	}
	dkst.localFlush()
	dkst.checkStore(backup_hash, num_entries, max_str_len)

	// Close it all off, make sure it is on the disk
	dkst.Close()
	log.Printf("Okay well the hash itself was consistent, but is it persistant?")

	// filename, overwrite, compact
	dkst1 := NewUrlMap(test_filename, false, false)

	dkst1.Flush()
	dkst1.checkStore(backup_hash, num_entries, max_str_len)
	dkst1.Flush()
	log.Println("1st Reload check complete")
	for i := 0; i < num_entries; i++ {
		str_len := rand.Int31n(int32(max_str_len)) + 1
		tst_string := RandStringBytesMaskImprSrc(int(str_len))
		dkst1.Set(NewUrl(tst_string))
		backup_hash[tst_string] = struct{}{}
	}
	dkst1.checkStore(backup_hash, num_entries, max_str_len)
	dkst1.Flush()
	dkst1.Close()

	dkst2 := NewUrlMap(test_filename, false, false)
	dkst2.checkStore(backup_hash, num_entries, max_str_len)
	log.Println("2nd Reload check complete")

	compact_filename := tempfilename()
	dkst2.dkst.ds.compact(compact_filename)

	dkst2.Close()

	dkst3 := NewUrlMap(compact_filename, false, false)
	dkst3.checkStore(backup_hash, num_entries, max_str_len)
	log.Printf("3rd reload. Check complete")
	dkst3.Close()
	os.Remove(test_filename)
	os.Remove(compact_filename)
}

func TestDiskStore0(t *testing.T) {
	test_filename := os.TempDir() + "/test.gkvlite"

	dkst := NewDkCollection(test_filename, true)
	dkst.SetAny(23, "Hello")
	dkst.SetAny(25, "Goodbye")
	dkst.ds.Flush()
	log.Println("We say:", dkst.GetString(23))
	log.Println("They say:", dkst.GetString(25))

	key_chan := dkst.GetIntKeys()
	for key := range key_chan {
		log.Println("The collection contains:", key)
	}
	dkst.ds.Close()
}

func TestDiskStore1(t *testing.T) {
	test_filename := os.TempDir() + "/test.gkvlite"

	dkst := NewDkCollection(test_filename, true)
	dkst.SetAny("Hello1", 23)
	dkst.SetAny("Goodbye1", 25)
	dkst.ds.Flush()
	log.Println("We say:", dkst.GetInt("Hello1"))
	log.Println("They say:", dkst.GetInt("Goodbye1"))

	key_chan := dkst.GetAnyKeys()
	for key := range key_chan {
		log.Println("The collection contains:", string(key))
	}
	dkst.ds.Close()
}

func TestDiskStore2(t *testing.T) {
	test_filename := os.TempDir() + "/test.gkvlite"

	dkst := NewDkCollection(test_filename, true)
	defer dkst.ds.Close()

	// Let's pretend to have some urls
	var url_0 Url
	var url_1 Url
	var url_2 Url
	url_0 = NewUrl("http://here.com")
	url_1 = NewUrl("http://there.com")
	url_2 = NewUrl("http://nowhere.com")
	if dkst.Size() > 0 {
		log.Fatal("Bigger than zero")
	}
	dkst.SetUrl(url_0)
	dkst.SetUrl(url_1)
	if dkst.Size() == 0 {
		log.Fatal("Size is zero")
	}

	if !dkst.Exist(url_0) {
		log.Fatal("0 does not exist")
	}
	if !dkst.Exist(url_1) {
		log.Fatal("1 does not exist")
	}
	if dkst.Exist(url_2) {
		log.Fatal("2 does exist")
	}
	dkst.ds.Flush()
	key_chan := dkst.GetAnyKeys()
	for key := range key_chan {
		log.Println("The collection contains:", string(key))
	}

	// Delete returns true if suceeds
	ok := dkst.Delete(url_0)
	if !ok {
		log.Fatal("Delete failed on url_0")
	}
	ok = dkst.Delete(url_1)
	if !ok {
		log.Fatal("Delete failed on url_1")
	}

	if dkst.Size() > 0 {
		log.Fatal("Bigger than zero after delete")
	}
}
func TestDiskStore3(t *testing.T) {
	test_filename := os.TempDir() + "/test.gkvlite"
	dkst := NewDkCollection(test_filename, true)
	defer dkst.ds.Close()
	// Let's pretend to have some urls
	var url_0 Url
	var url_1 Url
	var url_2 Url
	url_0 = NewUrl("http://here.com")
	url_0.SetPromiscuous()
	url_1 = NewUrl("http://there.com")
	url_2 = NewUrl("http://nowhere.com")
	dkst.SetUrl(url_0)
	dkst.SetUrl(url_1)
	dkst.SetUrl(url_2)
	output, err := url_0.goMarshalJSON()
	if err != nil {
		log.Fatal("Marshalling error", err)
	}
	if false {
		log.Println("JSON Output", string(output))
	}
	new_url0 := dkst.GetUrl("http://here.com")
	if !new_url0.GetPromiscuous() {
		log.Fatal("We should be promiscuous")
	}
	if new_url0.Url() != url_0.Url() {
		log.Fatal("URLs are incirrect", new_url0.Url(), url_0.Url())
	}
	new_url1 := dkst.GetUrl("http://there.com")
	new_url2 := dkst.GetUrl("http://nowhere.com")
	if new_url1.GetPromiscuous() {
		log.Fatal("We should not be promiscuous")
	}
	if new_url2.GetPromiscuous() {
		log.Fatal("We should not be promiscuous")
	}

}

func TestDiskRandom0(t *testing.T) {
	test_filename := os.TempDir() + "/test.gkvlite"

	dkst := NewDkCollection(test_filename, true)
	dkst.SetAny(23, "Hello")
	dkst.SetAny(24, "Goodbye")
	dkst.SetAny(25, "Goodbye")
	dkst.SetAny(26, "Goodbye")
	log.Println("We say:", dkst.GetString(23))
	log.Println("They say:", dkst.GetString(25))

	key_chan := dkst.GetAnyKeysRand()
	for key := range key_chan {
		log.Println("The collection contains:", string(key))
	}
	dkst.ds.Close()
}
