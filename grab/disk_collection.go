package grab

import (
	"log"
	"regexp"
	"sort"
	"strconv"

	"github.com/cbehopkins/gkvlite"
)

// Disk Collection of int to string map
type DkCollection struct {
	ds  *DkStore
	col *gkvlite.Collection
	re  *regexp.Regexp
}

func (dc *DkCollection) UseCollection(name string) {
	dc.col = dc.ds.st.GetCollection(name)
	if dc.col == nil {
		dc.col = dc.ds.st.SetCollection(name, nil)
	}
}
func NewDkCollection(filename string, overwrite bool) *DkCollection {
	itm := new(DkCollection)
	itm.re = regexp.MustCompile("(http[s]?://)([^/]*)")
	itm.ds = new(DkStore)
	itm.ds.openStore(filename, overwrite)
	itm.UseCollection("int_string")

	return itm
}

func (st *DkCollection) SetAny(key, val interface{}) {
	key_bs := toBa(key)
	val_bs := toBa(val)
	st.col.Set(key_bs, val_bs)
}

func (st *DkCollection) GetAny(key interface{}) []byte {
	key_bs := toBa(key)
	ret_val, err := st.col.Get(key_bs)
	check(err)
	return ret_val
}
func (st *DkCollection) GetUrl(key string) Url {
	return st.UrlFromBa([]byte(key))
}
func (st *DkCollection) SetUrl(in Url) {
	key := in.Key()
	key_ba := []byte(key)
	output, err := in.goMarshalJSON()
	if err == nil {
		st.SetAny(key_ba, output)
	} else {
		log.Fatal("JSON Marshall error")
	}
}
func (st *DkCollection) UrlFromBa(in []byte) Url {
	var itm Url
	var ok bool

	ok = st.Exist(in)
	if !ok {
		itm = NewUrlFromBa(in)
		return itm
	}
	itm_ba := st.GetAny(in)
	if string(itm_ba) == "" {
		return NewUrlFromBa(in)
	}
	//log.Fatal("Puzzling, this is not possible")
	var tmpUrl Url
	tmpUrl.goUnMarshalJSON(itm_ba)
	tmpUrl.UrlS = string(in)
	return tmpUrl
}
func (st *DkCollection) Delete(key interface{}) bool {
	key_bs := toBa(key)
	was_deleted, err := st.col.Delete(key_bs)
	check(err)
	return was_deleted
}

func (st *DkCollection) Exist(key interface{}) bool {
	key_bs := toBa(key)
	val, _ := st.col.GetItem(key_bs, false)
	return val != nil
}
func (st *DkCollection) GetString(key interface{}) string {
	return string(st.GetAny(key))
}
func (st *DkCollection) GetInt(key interface{}) int {
	val, err := strconv.Atoi(st.GetString(key))
	check(err)
	return val
}

func (st *DkCollection) GetIntKeys() (ret_chan chan int) {
	ret_chan = make(chan int)
	go func() {

		for ba := range st.GetAnyKeys() {
			key, err := strconv.Atoi(string(ba))
			check(err)
			ret_chan <- key
		}
		close(ret_chan)
	}()
	return ret_chan

}

func (st *DkCollection) GetAnyValues() (ret_chan chan []byte) {
	ret_chan = make(chan []byte)
	go func() {
		st.col.VisitItemsAscend([]byte(string(0)), true, func(i *gkvlite.Item) bool {
			// This visitor callback will be invoked with every item
			// If we want to stop visiting, return false;
			// otherwise return true to keep visiting.
			ret_chan <- st.GetAny(i.Key)
			return true
		})
		close(ret_chan)
	}()
	return ret_chan
}
func (st *DkCollection) GetAnyKeysArray(max_items int) (ret_array [][]byte) {
	ret_array = make([][]byte, 0, max_items)
	tmp_chan := make(chan []byte)
	go func() {
		cnt := 0
		min_itm, err := st.col.MinItem(true)
		check(err)
		st.col.VisitItemsAscend(min_itm.Key, true, func(i *gkvlite.Item) bool {
			// This visitor callback will be invoked with every item
			// If we want to stop visiting, return false;
			// otherwise return true to keep visiting.
			tmp_chan <- i.Key
			// keep going until we have got n items
			cnt++
			return cnt < max_items
		})
		close(tmp_chan)
	}()

	for v := range tmp_chan {
		ret_array = append(ret_array, v)
	}
	return ret_array
}
func (st *DkCollection) GetAnyKeys() (ret_chan chan []byte) {
	ret_chan = make(chan []byte)
	go func() {
		min_itm, err := st.col.MinItem(true)
		check(err)
		if min_itm != nil {
			st.col.VisitItemsAscend(min_itm.Key, true, func(i *gkvlite.Item) bool {
				// This visitor callback will be invoked with every item
				// If we want to stop visiting, return false;
				// otherwise return true to keep visiting.
				ret_chan <- i.Key
				return true
			})
		}
		close(ret_chan)
	}()
	return ret_chan
}
func (st *DkCollection) GetAnyKeysRand() (ret_chan chan []byte) {
	ret_chan = make(chan []byte)
	go func() {
		min_itm, err := st.col.MinItem(true)
		check(err)
		if min_itm != nil {
			st.col.VisitItemsAscend(min_itm.Key, true, func(i *gkvlite.Item) bool {
				// This visitor callback will be invoked with every item
				// If we want to stop visiting, return false;
				// otherwise return true to keep visiting.
				ret_chan <- i.Key
				return true
			})
		}
		close(ret_chan)
	}()
	return ret_chan
}
func (st *DkCollection) getMissingChan(max_items int, refr *TokenChan) chan string {
	uc := make(chan string)
	go st.getMissingChanWorker(uc, max_items, refr)
	return uc
}
func (st *DkCollection) getMissingChanWorker(url_chan chan string, max_items int, refr *TokenChan) {
	defer close(url_chan)
	// we will allow up to max_same of the same basename to go through
	// This is so we can fetch several things fromt he same host at once
	// But not too many!
	// It wants to be slightly more than the number of tokens we allow
	// for each basename
	if st == nil {
		log.Fatal("Invalid Stoer")
	}
	if st.col == nil {
		log.Fatal("Invalid collection")
	}
	max_same := max_items / 10 // fetch at least 10 domains worth to work with

	check_tk := false
	cnt := 0
	min_itm, err := st.col.MinItem(true)
	check(err)
	if min_itm == nil {
		return
	}
	unique_array := make(map[string]int)
	explore_func := func(i *gkvlite.Item) bool {
		// This visitor callback will be invoked with every item
		// If we want to stop visiting, return false;
		// otherwise return true to keep visiting.
		if i == nil {
			log.Fatal("WTF!")
		}
		tmp_val := string(i.Key)
		// If the token for this is in use
		// Then it won't fetch this pass, so don't let it through
		tmp_base := st.roughBase(tmp_val)
		if check_tk {
			ok := refr.BaseExist(tmp_base)
			if ok {
				// Keep going, look for something not in the map
				return true
			}
		}
		// We could have a bunch in the same base domain

		// We don't need exactly the correct and tollerant answer
		// a rough go will be fast enough
		val, ok := unique_array[tmp_base]
		if ok && (val >= max_same) {
			return true
		}
		if !ok {
			val = 0
		} else {
			val++
		}
		// Store it with the count of how many times we've visited
		unique_array[tmp_base] = val

		// We've found something interesting so increment the count
		cnt++
		// send the interesting thing
		//fmt.Println("Interesting item", tmp_val)
		url_chan <- tmp_val
		// keep going until we have got n items
		return cnt < max_items
	}
	st.col.VisitItemsAscend(min_itm.Key, true, explore_func)
}
func (st *DkCollection) GetMissing(max_items int, refr *TokenChan) (ret_map map[string]struct{}) {
	ret_map = make(map[string]struct{})
	for tmp_val := range st.getMissingChan(max_items, refr) {
		ret_map[tmp_val] = struct{}{}
	}
	return ret_map
}

func (st *DkCollection) Size() int {
	size := 0
	if st.col == nil {
		// The collection has not been set up yet
		return 0
	}
	min_itm, err := st.col.MinItem(true)
	check(err)
	if min_itm == nil {
		//mpty list if no minimum
		return size
	}
	st.col.VisitItemsAscend(min_itm.Key, true, func(i *gkvlite.Item) bool {
		// This visitor callback will be invoked with every item
		// If we want to stop visiting, return false;
		// otherwise return true to keep visiting.
		size++
		//mmediatly stops for performance
		return false
	})
	return size
}

func (st *DkCollection) Count() int {
	if st.col == nil {
		// The collection has not been set up yet
		return 0
	}
	numItems, _, err := st.col.GetTotals()
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

func (st *DkCollection) PrintWorkload() {
	min_itm, err := st.col.MinItem(true)
	check(err)
	unique_array := make(map[string]int)
	s := Spinner{scaler: 1000}
	var cnt int
	st.col.VisitItemsAscend(min_itm.Key, true, func(i *gkvlite.Item) bool {
		// This visitor callback will be invoked with every item
		// If we want to stop visiting, return false;
		// otherwise return true to keep visiting.
		tmp_val := string(i.Key)
		tmp_base := GetBase(tmp_val)
		val, ok := unique_array[tmp_base]
		if !ok {
			val = 0
		} else {
			val++
		}
		s.PrintSpin(cnt)
		cnt++
		// Store it with the count of how many times we've visited
		unique_array[tmp_base] = val

		return true
	})
	log.Println("Printing Current Workload")

	for _, itm := range rankByWordCount(unique_array) {
		key := itm.Key
		value := itm.Value
		log.Printf("%8d:%s\n", value, key)
	}
}
