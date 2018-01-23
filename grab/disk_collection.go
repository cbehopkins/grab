package grab

import (
	"log"
	"regexp"
	"sort"
	"strconv"

	"fmt"

	"github.com/cbehopkins/gkvlite"
)

// DkCollection Disk Collection
// Wraps up the dis store and colleciton into a usable whole
type DkCollection struct {
	ds  *DkStore
	col *gkvlite.Collection
	re  *regexp.Regexp
}

// UseCollection specifies the name of the colleciton we should use
func (dc *DkCollection) UseCollection(name string) {
	dc.col = dc.ds.st.GetCollection(name)
	if dc.col == nil {
		dc.col = dc.ds.st.SetCollection(name, nil)
	}
}

// NewDkCollection - create a new collection
// at the specified filename
// overwriting if needed
func NewDkCollection(filename string, overwrite bool) *DkCollection {
	itm := new(DkCollection)
	itm.re = regexp.MustCompile("(http[s]?://)([^/]*)")
	itm.ds = new(DkStore)
	itm.ds.openStore(filename, overwrite)
	itm.UseCollection("int_string")

	return itm
}

// SetAny - Set the value of any type
func (dc *DkCollection) SetAny(key, val interface{}) {
	dc.col.SetAny(key, val)
}

// GetAny - Get a value of any type
func (dc *DkCollection) GetAny(key interface{}) []byte {
	retVal, err := dc.col.GetAny(key)
	check(err)
	return retVal
}

// GetURL returns the full url structure stored in the colleciton
func (dc *DkCollection) GetURL(key string) URL {
	return dc.URLFromBa([]byte(key))
}

// SetURL sets the url as one in the collection
func (dc *DkCollection) SetURL(in URL) {
	key := in.Key()
	keyBa := []byte(key)
	output, err := in.goMarshalJSON()
	if err == nil {
		dc.SetAny(keyBa, output)
	} else {
		log.Fatal("JSON Marshall error")
	}
}

// URLFromBa Creates a new URL from the supplied byte array
func (dc *DkCollection) URLFromBa(in []byte) URL {
	var itm URL
	var ok bool

	ok = dc.Exist(in)
	if !ok {
		itm = NewURLFromBa(in)
		return itm
	}
	itmBa := dc.GetAny(in)
	return dc.URLFromVals(in, itmBa)
}

// URLFromVals Creates a new URL from the things we have retrieved from disk
func (dc *DkCollection) URLFromVals(in, itmBa []byte) URL {
	if string(itmBa) == "" {
		return NewURLFromBa(in)
	}
	var tmpURL URL
	tmpURL.goUnMarshalJSON(itmBa)
	tmpURL.URLs = string(in)
	tmpURL.Initialise()
	return tmpURL
}

// Delete returns true if we managed to delete the supplied key
func (dc *DkCollection) Delete(key interface{}) bool {
	wasDeleted, err := dc.col.DeleteAny(key)
	check(err)
	return wasDeleted
}

// Exist returns true if a supplied key exists
func (dc *DkCollection) Exist(key interface{}) bool {
	return dc.col.ExistAny(key)
}

// GetIntKeys get keys of integer type
func (dc *DkCollection) GetIntKeys() (retChan chan int) {
	retChan = make(chan int)
	go func() {

		for ba := range dc.GetAnyKeys() {
			key, err := strconv.Atoi(string(ba))
			check(err)
			retChan <- key
		}
		close(retChan)
	}()
	return retChan

}


// GetAnyKeysArray Get keys of any type
// return in an array
func (dc *DkCollection) GetAnyKeysArray(maxItems int) (retArray [][]byte) {
	retArray = make([][]byte, 0, maxItems)
	tmpChan := make(chan []byte)
	go func() {
		cnt := 0
		minItm, err := dc.col.MinItem(true)
		check(err)
		dc.col.VisitItemsAscend(minItm.Key, true, func(i *gkvlite.Item) bool {
			// This visitor callback will be invoked with every item
			// If we want to stop visiting, return false;
			// otherwise return true to keep visiting.
			tmpChan <- i.Key
			// keep going until we have got n items
			cnt++
			return cnt < maxItems
		})
		close(tmpChan)
	}()

	for v := range tmpChan {
		retArray = append(retArray, v)
	}
	return retArray
}

// GetAnyKeysArrayFrom Get keys of any type
// return in an array
// starting the search From the specified location
func (dc *DkCollection) GetAnyKeysArrayFrom(maxItems int, start []byte) (retArray [][]byte) {
	retArray = make([][]byte, 0, maxItems)
	tmpChan := make(chan []byte)
	go func() {
		defer close(tmpChan)
		startItm, err := dc.col.GetItem(start, false)
		if (startItm == nil) || (err != nil) {
			startItm, err = dc.col.MinItem(true)
			check(err)
			if startItm == nil {
				if dc.Size() > 0 {
					log.Fatal("No start item for:")
				}
				// Empty collection
				return
			}
		}
		cnt := 0
		dc.col.VisitItemsAscend(startItm.Key, true, func(i *gkvlite.Item) bool {
			// This visitor callback will be invoked with every item
			// If we want to stop visiting, return false;
			// otherwise return true to keep visiting.
			tmpChan <- i.Key
			// keep going until we have got n items
			cnt++
			return cnt < maxItems
		})
	}()

	for v := range tmpChan {
		retArray = append(retArray, v)
	}
	return retArray
}
// GetURLArrayFrom Get an array of URLs starting at the specified location
func (dc *DkCollection) GetURLArrayFrom(maxItems int, start URL) (retArray []URL) {
	retArray = make([]URL, 0, maxItems)
	tmpChan := make(chan URL)
	go func() {
		defer close(tmpChan)
		startItm, err := dc.col.GetItem(start.ToBa(), false)
		if (startItm == nil) || (err != nil) {
			startItm, err = dc.col.MinItem(true)
			check(err)
			if startItm == nil {
				if dc.Size() > 0 {
					log.Fatal("No start item for:")
				}
				// Empty collection
				return
			}
		}
		cnt := 0
		dc.col.VisitItemsAscend(startItm.Key, true, func(i *gkvlite.Item) bool {
			// This visitor callback will be invoked with every item
			// If we want to stop visiting, return false;
			// otherwise return true to keep visiting.
			urlV := i.Key
			urlP := i.Val
			// REVISIT - Throw this processing into a separate routine
			url := dc.URLFromVals(urlV, urlP)
			tmpChan <- url
			// keep going until we have got n items
			cnt++
			return cnt < maxItems
		})
	}()

	for v := range tmpChan {
		retArray = append(retArray, v)
	}
	return retArray
}

// GetAnyKeys Get keys of any data type
func (dc *DkCollection) GetAnyKeys() (retChan chan []byte) {
	retChan = make(chan []byte)
	go func() {
		minItm, err := dc.col.MinItem(true)
		check(err)
		if minItm != nil {
			dc.col.VisitItemsAscend(minItm.Key, true, func(i *gkvlite.Item) bool {
				// This visitor callback will be invoked with every item
				// If we want to stop visiting, return false;
				// otherwise return true to keep visiting.
				retChan <- i.Key
				return true
			})
		}
		close(retChan)
	}()
	return retChan
}

// GetAnyKeysRand Ger keys of any type in a random order
// This is a work in progress
func (dc *DkCollection) GetAnyKeysRand() (retChan chan []byte) {
	retChan = make(chan []byte)
	go func() {
		minItm, err := dc.col.MinItem(true)
		check(err)
		if minItm != nil {
			dc.col.VisitItemsAscend(minItm.Key, true, func(i *gkvlite.Item) bool {
				// This visitor callback will be invoked with every item
				// If we want to stop visiting, return false;
				// otherwise return true to keep visiting.
				retChan <- i.Key
				return true
			})
		}
		close(retChan)
	}()
	return retChan
}
func (dc *DkCollection) getMissingChan(maxItems int, refr *TokenChan) chan string {
	uc := make(chan string)
	go dc.getMissingChanWorker(uc, maxItems, refr)
	return uc
}
func (dc *DkCollection) getMissingChanWorker(urlChan chan string, maxItems int, refr *TokenChan) {
	defer close(urlChan)
	// we will allow up to max_same of the same basename to go through
	// This is so we can fetch several things fromt he same host at once
	// But not too many!
	// It wants to be slightly more than the number of tokens we allow
	// for each basename
	if dc == nil {
		log.Fatal("Invalid Stoer")
	}
	if dc.col == nil {
		log.Fatal("Invalid collection")
	}
	maxSame := maxItems / 10 // fetch at least 10 domains worth to work with

	checkTk := false
	cnt := 0
	minItm, err := dc.col.MinItem(true)
	check(err)
	if minItm == nil {
		return
	}
	uniqueArray := make(map[string]int)
	exploreFunc := func(i *gkvlite.Item) bool {
		// This visitor callback will be invoked with every item
		// If we want to stop visiting, return false;
		// otherwise return true to keep visiting.
		if i == nil {
			log.Fatal("WTF!")
		}
		tmpVal := string(i.Key)
		// If the token for this is in use
		// Then it won't fetch this pass, so don't let it through
		tmpBase := dc.roughBase(tmpVal)
		if checkTk {
			ok := refr.BaseExist(tmpBase)
			if ok {
				// Keep going, look for something not in the map
				return true
			}
		}
		// We could have a bunch in the same base domain

		// We don't need exactly the correct and tollerant answer
		// a rough go will be fast enough
		val, ok := uniqueArray[tmpBase]
		if ok && (val >= maxSame) {
			return true
		}
		if !ok {
			val = 0
		} else {
			val++
		}
		// Store it with the count of how many times we've visited
		uniqueArray[tmpBase] = val

		// We've found something interesting so increment the count
		cnt++
		// send the interesting thing
		//fmt.Println("Interesting item", tmp_val)
		urlChan <- tmpVal
		// keep going until we have got n items
		return cnt < maxItems
	}
	dc.col.VisitItemsAscend(minItm.Key, true, exploreFunc)
}

// GetMissing items from the collection
// i.e. populate a return map with at most maxItems, moderated by tokenChan
func (dc *DkCollection) GetMissing(maxItems int, refr *TokenChan) (retMap map[string]struct{}) {
	retMap = make(map[string]struct{})
	for tmpVal := range dc.getMissingChan(maxItems, refr) {
		retMap[tmpVal] = struct{}{}
	}
	return retMap
}

// Size returns the size of the collection
func (dc *DkCollection) Size() int {
	size := 0
	if dc.col == nil {
		// The collection has not been set up yet
		return 0
	}
	minItm, err := dc.col.MinItem(true)
	check(err)
	if minItm == nil {
		//mpty list if no minimum
		return size
	}
	dc.col.VisitItemsAscend(minItm.Key, true, func(i *gkvlite.Item) bool {
		// This visitor callback will be invoked with every item
		// If we want to stop visiting, return false;
		// otherwise return true to keep visiting.
		size++
		//mmediatly stops for performance
		return false
	})
	return size
}

// Count the number of items in the collection
func (dc *DkCollection) Count() int {
	if dc.col == nil {
		// The collection has not been set up yet
		return 0
	}
	numItems, _, err := dc.col.GetTotals()
	check(err)
	return int(numItems)
}

func rankByWordCount(wordFrequencies map[string]int) PairList {
	pl := make(PairList, len(wordFrequencies))
	i := 0
	for k, v := range wordFrequencies {
		pl[i] = Pair{k, v}
		i++
	}
	sort.Sort(pl)
	return pl
}

// Workload - output the current workload
func (dc *DkCollection) Workload(spin bool) string {
	minItm, err := dc.col.MinItem(true)
	check(err)
	uniqueArray := make(map[string]int)
	s := Spinner{scaler: 1000}
	var cnt int
	dc.col.VisitItemsAscend(minItm.Key, true, func(i *gkvlite.Item) bool {
		// This visitor callback will be invoked with every item
		// If we want to stop visiting, return false;
		// otherwise return true to keep visiting.
		tmpVal := string(i.Key)
		tmpBase := GetBase(tmpVal)
		val, ok := uniqueArray[tmpBase]
		if !ok {
			val = 0
		} else {
			val++
		}
		if spin {
			s.PrintSpin(cnt)
		}
		cnt++
		// Store it with the count of how many times we've visited
		uniqueArray[tmpBase] = val

		return true
	})
	var retString string
	for _, itm := range rankByWordCount(uniqueArray) {
		key := itm.Key
		value := itm.Value
		retString += fmt.Sprintf("%8d:%s\n", value, key)
	}
	return retString
}

// PrintWorkload output the cirrent workload
func (dc *DkCollection) PrintWorkload() {
	log.Println("Printing Current Workload")
	log.Printf(dc.Workload(true))

}
