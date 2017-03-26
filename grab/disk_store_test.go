package grab

import (
	"log"
	"math/rand"
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
	for v, _ := range backup_hash {
		if !dkst.Exist(Url(v)) {
			log.Fatal("Error, missing key from disk", v)
		}
	}
	for v := range dkst.VisitAll() {
		_, ok := backup_hash[string(v)]
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
			if dkst.Exist(Url(tst_string)) {
				log.Fatalf("%s is in dkst, but not in bakup\n", tst_string)
			}
		} else {
			// try again
			i--
		}
	}
}

func (dkst *DkStore) checkStore(backup_hash map[string]struct{}, num_entries, max_str_len int) {

	for v, _ := range backup_hash {
		if !dkst.Exist(v) {
			log.Fatal("Error, missing key from disk", v)
		}
	}
	for v := range dkst.GetStringKeys() {
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

func TestDiskPersist0(t *testing.T) {
	max_str_len := 256
	num_entries := 100
	dkst := NewDkStore("/tmp/test.gkvlite", true)
	backup_hash := make(map[string]struct{})

	// Create some random entries of varing lengths
	for i := 0; i < num_entries; i++ {
		str_len := rand.Int31n(int32(max_str_len)) + 1
		tst_string := RandStringBytesMaskImprSrc(int(str_len))
		dkst.SetAny(tst_string, "")
		backup_hash[tst_string] = struct{}{}
	}
	dkst.Flush()
	dkst.checkStore(backup_hash, num_entries, max_str_len)

	dkst.Sync()
	dkst.Close()
	log.Printf("Okay well the hash itself was consistent, but is it persistant?")

	dkst1 := NewDkStore("/tmp/test.gkvlite", false)
	dkst1.checkStore(backup_hash, num_entries, max_str_len)
}
func TestDiskPersist1(t *testing.T) {
	max_str_len := 256
	num_entries := 100000
	dkst := NewUrlMap("/tmp/test.gkvlite", true)
	backup_hash := make(map[string]struct{})

	// Create some random entries of varing lengths
	for i := 0; i < num_entries; i++ {
		str_len := rand.Int31n(int32(max_str_len)) + 1
		tst_string := RandStringBytesMaskImprSrc(int(str_len))
		dkst.Set(Url(tst_string))
		backup_hash[tst_string] = struct{}{}
	}
	dkst.checkStore(backup_hash, num_entries, max_str_len)

	// Close it all off, make sure it is on the disk
	dkst.Close()
	log.Printf("Okay well the hash itself was consistent, but is it persistant?")

	dkst1 := NewUrlMap("/tmp/test.gkvlite", false)
	dkst1.checkStore(backup_hash, num_entries, max_str_len)
	dkst1.Close()

}
func TestDiskPersist2(t *testing.T) {
	max_str_len := 256
	num_entries := 100000
	dkst := NewUrlMap("/tmp/test.gkvlite", true)
	backup_hash := make(map[string]struct{})

	// Create some random entries of varing lengths
	for i := 0; i < num_entries; i++ {
		str_len := rand.Int31n(int32(max_str_len)) + 1
		tst_string := RandStringBytesMaskImprSrc(int(str_len))
		dkst.Set(Url(tst_string))
		backup_hash[tst_string] = struct{}{}
	}
	dkst.checkStore(backup_hash, num_entries, max_str_len)

	// Close it all off, make sure it is on the disk
	dkst.Close()
	log.Printf("Okay well the hash itself was consistent, but is it persistant?")

	dkst1 := NewUrlMap("/tmp/test.gkvlite", false)
	dkst1.checkStore(backup_hash, num_entries, max_str_len)

	// Create some more random entries of varing lengths
	for i := 0; i < num_entries; i++ {
		str_len := rand.Int31n(int32(max_str_len)) + 1
		tst_string := RandStringBytesMaskImprSrc(int(str_len))
		dkst1.Set(Url(tst_string))
		backup_hash[tst_string] = struct{}{}
	}
	dkst1.checkStore(backup_hash, num_entries, max_str_len)

	dkst1.Close()
	// This should work,
	dkst2 := NewUrlMap("/tmp/test.gkvlite", false)
	dkst2.checkStore(backup_hash, num_entries, max_str_len)
	dkst2.Close()
}

func TestDiskStore0(t *testing.T) {

	dkst := NewDkStore("/tmp/test.gkvlite", true)
	dkst.SetAny(23, "Hello")
	dkst.SetAny(25, "Goodbye")
	dkst.Flush()
	log.Println("We say:", dkst.GetString(23))
	log.Println("They say:", dkst.GetString(25))

	key_chan := dkst.GetIntKeys()
	for key := range key_chan {
		log.Println("The collection contains:", key)
	}
	dkst.Close()
}

func TestDiskStore1(t *testing.T) {

	dkst := NewDkStore("/tmp/test.gkvlite", true)
	dkst.SetAny("Hello1", 23)
	dkst.SetAny("Goodbye1", 25)
	dkst.Flush()
	log.Println("We say:", dkst.GetInt("Hello1"))
	log.Println("They say:", dkst.GetInt("Goodbye1"))

	key_chan := dkst.GetAnyKeys()
	for key := range key_chan {
		log.Println("The collection contains:", string(key))
	}
	dkst.Close()
}

func TestDiskStore2(t *testing.T) {

	dkst := NewDkStore("/tmp/test.gkvlite", true)
	defer dkst.Close()

	// Let's pretend to have some urls
	var url_0 Url
	var url_1 Url
	var url_2 Url
	url_0 = "http://here.com"
	url_1 = "http://there.com"
	url_2 = "http://nowhere.com"
	if dkst.Size() > 0 {
		log.Fatal("Bigger than zero")
	}
	dkst.SetAny(url_0, "")
	dkst.SetAny(url_1, "")
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
	dkst.Flush()
	key_chan := dkst.GetAnyKeys()
	for key := range key_chan {
		log.Println("The collection contains:", string(key))
	}

	// Delete returns true if suceeds
	_ = dkst.Delete(url_0)
	_ = dkst.Delete(url_1)

	if dkst.Size() > 0 {
		log.Fatal("Bigger than zero after delete")
	}

}
