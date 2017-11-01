package grab

import (
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/cbehopkins/gkvlite"
)

func GetKeysChan(st *gkvlite.Collection) (retChan chan string) {
	retChan = make(chan string)
	go func() {
		st.VisitItemsAscend([]byte(string(0)), true, func(i *gkvlite.Item) bool {
			// This visitor callback will be invoked with every item
			// If we want to stop visiting, return false;
			// otherwise return true to keep visiting.
			retChan <- string(i.Key)
			return true
		})
		close(retChan)
	}()
	return retChan
}
func ExistsSt(st *gkvlite.Collection, key string) bool {
	keyBs := []byte(key)
	existsVal, _ := st.GetItem(keyBs, false)
	return existsVal != nil
}

func checkStoreN(st *gkvlite.Collection, backupHash map[string]struct{}, numEntries, maxStrLen int) {

	for v := range backupHash {
		if !ExistsSt(st, v) {
			log.Fatal("Error, missing key from disk", v)
		}
	}
	for v := range GetKeysChan(st) {
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
			if ExistsSt(st, tstString) {
				log.Fatalf("%s is in dkst, but not in bakup\n", tstString)
			}
		} else {
			// try again
			i--
		}
	}
}

// Test function to let me debug the internale
func TestGkvFunc(t *testing.T) {
	testFilen := tempfilename("", false)
	defer rmFilename(testFilen)
	if useTestParallel {
		t.Parallel()
	}
	rmFilename(testFilen)

	colName := "tst"
	f, err := os.Create(testFilen)
	check(err)
	st, err := gkvlite.NewStore(f)
	check(err)
	col := st.SetCollection(colName, nil)
	col.Set([]byte("a"), []byte{})
	col.Set([]byte("b"), []byte{})
	col.Set([]byte("c"), []byte{})
	col.Set([]byte("d"), []byte{})
	col.Set([]byte("e"), []byte{})
	col.Set([]byte("f"), []byte{})
	col.VisitItemsAscend([]byte(string(0)), true, func(i *gkvlite.Item) bool {
		log.Printf("Key:%s\n", string(i.Key))
		return true
	})
}
func TestGkv0(t *testing.T) {
	maxStrLen := 256
	numEntries := 1000

	testFilen := tempfilename("", false)
	defer rmFilename(testFilen)
	if useTestParallel {
		t.Parallel()
	}
	rmFilename(testFilen)

	colName := "tst"
	f, err := os.Create(testFilen)
	check(err)
	st, err := gkvlite.NewStore(f)
	check(err)
	col := st.SetCollection(colName, nil)

	backupHash := make(map[string]struct{})

	// Create some random entries of varing lengths
	for i := 0; i < numEntries; i++ {
		strLen := rand.Int31n(int32(maxStrLen)) + 1
		tstString := RandStringBytesMaskImprSrc(int(strLen))
		col.Set([]byte(tstString), []byte{})
		backupHash[tstString] = struct{}{}
	}
	checkStoreN(col, backupHash, numEntries, maxStrLen)

	// Close it all off, make sure it is on the disk
	st.Flush()
	st.Close()
	f.Sync()
	f.Close()
	log.Printf("Okay well the hash itself was consistent, but is it persistant?")

	f1, err := os.OpenFile(testFilen, os.O_RDWR, 0600)
	check(err)
	if f1 == nil {
		log.Fatal("F1 descriptor is nil")
	}
	st1, err := gkvlite.NewStore(f1)
	check(err)
	if st1 == nil {
		log.Fatal("st1 was nil")
	}
	if st1.GetCollectionNames()[0] != colName {
		log.Fatal("Collection not in as expected")
	}
	err = st1.Flush()
	check(err)

	col1 := st1.GetCollection(colName)
	if col1 == nil {
		log.Fatal("col1 was nil")
	}

	checkStoreN(col1, backupHash, numEntries, maxStrLen)
	log.Println("1st Reload check complete")
	if true {
		for i := 0; i < numEntries; i++ {
			strLen := rand.Int31n(int32(maxStrLen)) + 1
			tstString := RandStringBytesMaskImprSrc(int(strLen))
			col1.Set([]byte(tstString), []byte{})
			backupHash[tstString] = struct{}{}
		}
		checkStoreN(col1, backupHash, numEntries, maxStrLen)
		log.Println("1st Insertion check complete")
	}
	// Close everything down
	err = st1.Flush()
	check(err)
	st1.Close()
	f1.Sync()
	f1.Close()

	f2, err := os.Open(testFilen)
	check(err)
	st2, err := gkvlite.NewStore(f2)
	check(err)
	if st2 == nil {
		log.Fatal("st2 was nil")
	}
	if st2.GetCollectionNames()[0] != colName {
		log.Fatal("Collection not in as expected")
	}

	col2 := st2.GetCollection(colName)
	if col2 == nil {
		log.Fatal("col2 was nil")
	}
	log.Println("Starting 2nd Reload Check")
	checkStoreN(col2, backupHash, numEntries, maxStrLen)
	log.Println("2nd Reload check complete")
	st2.Flush()
	st2.Close()
	f2.Sync()
	f2.Close()
}
func TestGkv1(t *testing.T) {
	maxStrLen := 256
	numEntries := 50000

	testFilen := tempfilename("", false)
	defer rmFilename(testFilen)
	if useTestParallel {
		t.Parallel()
	}
	rmFilename(testFilen)

	colName := "tst"
	f, err := os.Create(testFilen)
	check(err)
	st, err := gkvlite.NewStore(f)
	check(err)
	col := st.SetCollection(colName, nil)

	backupHash := make(map[string]struct{})

	// Create some random entries of varing lengths
	for i := 0; i < numEntries; i++ {
		strLen := rand.Int31n(int32(maxStrLen)) + 1
		tstString := RandStringBytesMaskImprSrc(int(strLen))
		col.Set([]byte(tstString), []byte{})
		backupHash[tstString] = struct{}{}
	}
	//log.Printf("Store created, checking self")
	checkStoreN(col, backupHash, numEntries, maxStrLen)

	// Close it all off, make sure it is on the disk
	st.Flush()
	st.Close()
	f.Sync()
	f.Close()
	log.Printf("Okay well the hash itself was consistent, but is it persistant?")

	f1, err := os.OpenFile(testFilen, os.O_RDWR, 0600)
	check(err)
	if f1 == nil {
		log.Fatal("F1 descriptor is nil")
	}
	st1, err := gkvlite.NewStore(f1)
	check(err)
	if st1 == nil {
		log.Fatal("st1 was nil")
	}
	if st1.GetCollectionNames()[0] != colName {
		log.Fatal("Collection not in as expected")
	}
	err = st1.Flush()
	check(err)

	col1 := st1.GetCollection(colName)
	if col1 == nil {
		log.Fatal("col1 was nil")
	}

	checkStoreN(col1, backupHash, numEntries, maxStrLen)
	log.Println("1st Reload check complete")
	if true {
		for i := 0; i < numEntries; i++ {
			strLen := rand.Int31n(int32(maxStrLen)) + 1
			tstString := RandStringBytesMaskImprSrc(int(strLen))
			col1.Set([]byte(tstString), []byte{})
			backupHash[tstString] = struct{}{}
		}
		checkStoreN(col1, backupHash, numEntries, maxStrLen)
		log.Println("1st Insertion check complete")
	}
	// Close everything down
	err = st1.Flush()
	check(err)
	st1.Close()
	f1.Sync()
	f1.Close()

	f2, err := os.Open(testFilen)
	check(err)
	st2, err := gkvlite.NewStore(f2)
	check(err)
	if st2 == nil {
		log.Fatal("st2 was nil")
	}
	if st2.GetCollectionNames()[0] != colName {
		log.Fatal("Collection not in as expected")
	}

	col2 := st2.GetCollection(colName)
	if col2 == nil {
		log.Fatal("col2 was nil")
	}
	log.Println("Starting 2nd Reload Check")
	checkStoreN(col2, backupHash, numEntries, maxStrLen)
	log.Println("2nd Reload check complete")
	st2.Flush()
	st2.Close()
	f2.Sync()
	f2.Close()
}
