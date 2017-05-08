package grab

import (
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/steveyen/gkvlite"
)

func GetKeysChan(st *gkvlite.Collection) (ret_chan chan string) {
	ret_chan = make(chan string)
	go func() {
		st.VisitItemsAscend([]byte(string(0)), true, func(i *gkvlite.Item) bool {
			// This visitor callback will be invoked with every item
			// If we want to stop visiting, return false;
			// otherwise return true to keep visiting.
			ret_chan <- string(i.Key)
			return true
		})
		close(ret_chan)
	}()
	return ret_chan
}
func ExistsSt(st *gkvlite.Collection, key string) bool {
	key_bs := []byte(key)
	exists_val, _ := st.GetItem(key_bs, false)
	return exists_val != nil
}

func checkStoreN(st *gkvlite.Collection, backup_hash map[string]struct{}, num_entries, max_str_len int) {

	for v, _ := range backup_hash {
		if !ExistsSt(st, v) {
			log.Fatal("Error, missing key from disk", v)
		}
	}
	for v := range GetKeysChan(st) {
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
			if ExistsSt(st, tst_string) {
				log.Fatalf("%s is in dkst, but not in bakup\n", tst_string)
			}
		} else {
			// try again
			i--
		}
	}
}

func TestGkv0(t *testing.T) {
	max_str_len := 256
	num_entries := 1000
	test_filen := "/tmp/test.gkvlite"
	col_name := "tst"
	f, err := os.Create(test_filen)
	check(err)
	st, err := gkvlite.NewStore(f)
	check(err)
	col := st.SetCollection(col_name, nil)

	backup_hash := make(map[string]struct{})

	// Create some random entries of varing lengths
	for i := 0; i < num_entries; i++ {
		str_len := rand.Int31n(int32(max_str_len)) + 1
		tst_string := RandStringBytesMaskImprSrc(int(str_len))
		col.Set([]byte(tst_string), []byte{})
		backup_hash[tst_string] = struct{}{}
	}
	checkStoreN(col, backup_hash, num_entries, max_str_len)

	// Close it all off, make sure it is on the disk
	st.Flush()
	st.Close()
	f.Sync()
	f.Close()
	log.Printf("Okay well the hash itself was consistent, but is it persistant?")

	f1, err := os.OpenFile(test_filen, os.O_RDWR, 0600)
	check(err)
	if f1 == nil {
		log.Fatal("F1 descriptor is nil")
	}
	st1, err := gkvlite.NewStore(f1)
	check(err)
	if st1 == nil {
		log.Fatal("st1 was nil")
	}
	if st1.GetCollectionNames()[0] != col_name {
		log.Fatal("Collection not in as expected")
	}
	err = st1.Flush()
	check(err)

	col1 := st1.GetCollection(col_name)
	if col1 == nil {
		log.Fatal("col1 was nil")
	}

	checkStoreN(col1, backup_hash, num_entries, max_str_len)
	log.Println("1st Reload check complete")
	if true {
		for i := 0; i < num_entries; i++ {
			str_len := rand.Int31n(int32(max_str_len)) + 1
			tst_string := RandStringBytesMaskImprSrc(int(str_len))
			col1.Set([]byte(tst_string), []byte{})
			backup_hash[tst_string] = struct{}{}
		}
		checkStoreN(col1, backup_hash, num_entries, max_str_len)
		log.Println("1st Insertion check complete")
	}
	// Close everything down
	err = st1.Flush()
	check(err)
	st1.Close()
	f1.Sync()
	f1.Close()

	f2, err := os.Open(test_filen)
	check(err)
	st2, err := gkvlite.NewStore(f2)
	check(err)
	if st2 == nil {
		log.Fatal("st2 was nil")
	}
	if st2.GetCollectionNames()[0] != col_name {
		log.Fatal("Collection not in as expected")
	}

	col2 := st2.GetCollection(col_name)
	if col2 == nil {
		log.Fatal("col2 was nil")
	}
	log.Println("Starting 2nd Reload Check")
	checkStoreN(col2, backup_hash, num_entries, max_str_len)
	log.Println("2nd Reload check complete")
	st2.Flush()
	st2.Close()
	f2.Sync()
	f2.Close()
}
func TestGkv1(t *testing.T) {
	max_str_len := 256
	num_entries := 100000
	test_filen := "/tmp/test.gkvlite"
	col_name := "tst"
	f, err := os.Create(test_filen)
	check(err)
	st, err := gkvlite.NewStore(f)
	check(err)
	col := st.SetCollection(col_name, nil)

	backup_hash := make(map[string]struct{})

	// Create some random entries of varing lengths
	for i := 0; i < num_entries; i++ {
		str_len := rand.Int31n(int32(max_str_len)) + 1
		tst_string := RandStringBytesMaskImprSrc(int(str_len))
		col.Set([]byte(tst_string), []byte{})
		backup_hash[tst_string] = struct{}{}
	}
	checkStoreN(col, backup_hash, num_entries, max_str_len)

	// Close it all off, make sure it is on the disk
	st.Flush()
	st.Close()
	f.Sync()
	f.Close()
	log.Printf("Okay well the hash itself was consistent, but is it persistant?")

	f1, err := os.OpenFile(test_filen, os.O_RDWR, 0600)
	check(err)
	if f1 == nil {
		log.Fatal("F1 descriptor is nil")
	}
	st1, err := gkvlite.NewStore(f1)
	check(err)
	if st1 == nil {
		log.Fatal("st1 was nil")
	}
	if st1.GetCollectionNames()[0] != col_name {
		log.Fatal("Collection not in as expected")
	}
	err = st1.Flush()
	check(err)

	col1 := st1.GetCollection(col_name)
	if col1 == nil {
		log.Fatal("col1 was nil")
	}

	checkStoreN(col1, backup_hash, num_entries, max_str_len)
	log.Println("1st Reload check complete")
	if true {
		for i := 0; i < num_entries; i++ {
			str_len := rand.Int31n(int32(max_str_len)) + 1
			tst_string := RandStringBytesMaskImprSrc(int(str_len))
			col1.Set([]byte(tst_string), []byte{})
			backup_hash[tst_string] = struct{}{}
		}
		checkStoreN(col1, backup_hash, num_entries, max_str_len)
		log.Println("1st Insertion check complete")
	}
	// Close everything down
	err = st1.Flush()
	check(err)
	st1.Close()
	f1.Sync()
	f1.Close()

	f2, err := os.Open(test_filen)
	check(err)
	st2, err := gkvlite.NewStore(f2)
	check(err)
	if st2 == nil {
		log.Fatal("st2 was nil")
	}
	if st2.GetCollectionNames()[0] != col_name {
		log.Fatal("Collection not in as expected")
	}

	col2 := st2.GetCollection(col_name)
	if col2 == nil {
		log.Fatal("col2 was nil")
	}
	log.Println("Starting 2nd Reload Check")
	checkStoreN(col2, backup_hash, num_entries, max_str_len)
	log.Println("2nd Reload check complete")
	st2.Flush()
	st2.Close()
	f2.Sync()
	f2.Close()
}
