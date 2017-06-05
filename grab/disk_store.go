package grab

import (
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"

	"github.com/cbehopkins/gkvlite"
)

var UseConcSafe = false

type DkStFileIf interface {
	ReadAt([]byte, int64) (int, error)
	WriteAt([]byte, int64) (int, error)
	Truncate(size int64) error
	Sync() error
	Close() error
	Stat() (os.FileInfo, error)
}

// Disk Store of int to string map
type DkStore struct {
	st *gkvlite.Store
	is *gkvlite.Collection
	f  DkStFileIf
	re *regexp.Regexp
}

func (ds *DkStore) Flush() {
	err := ds.st.Flush()
	check(err)
}
func (ds *DkStore) Sync() {
	err := ds.f.Sync()
	check(err)
}
func (ds *DkStore) Close() {
	err := ds.st.Flush()
	check(err)
	ds.st.Close()
	check(err)
	err = ds.f.Sync()
	check(err)
	err = ds.f.Close()
	check(err)
}
func newStore(filename string) (DkStFileIf, *gkvlite.Store) {
	st := new(gkvlite.Store)
	var err error
	var f DkStFileIf
	// Remember to close the file later

	// If the file does not exiast already
	if UseConcSafe {
		f, err = NewConcSafe(filename)
	} else {
		f, err = os.Create(filename)
	}
	check(err)
	st, err = gkvlite.NewStore(f)
	check(err)
	if st == nil {
		log.Fatal("Store creation error - gkvlite.NewStore returned nil")
	}
	return f, st
}
func (ds *DkStore) compact(filename string) {
	f, err := os.Create(filename)
	check(err)
	ns, err := ds.st.CopyTo(f, 10)
	check(err)
	ns.Flush()
	ns.Close()
	f.Sync()
	f.Close()
}
func NewDkStore(filename string, overwrite bool) *DkStore {
	itm := new(DkStore)
	if _, err := os.Stat(filename); overwrite || os.IsNotExist(err) {
		// Doesn't exist? Happy days, create it
		f, st := newStore(filename)
		itm.st = st
		itm.f = f
		itm.is = itm.st.SetCollection("int_string", nil)
		itm.re = regexp.MustCompile("(http[s]?://)([^/]*)")
		return itm
	} else {
		var t_f DkStFileIf
		var err error
		if UseConcSafe {
			t_f, err = OpenConcSafe(filename)
		} else {
			t_f, err = os.OpenFile(filename, os.O_RDWR, 0600)
		}
		check(err)

		t_st, err := gkvlite.NewStore(t_f)
		check(err)
		if t_st == nil {
			log.Fatal("Nil t_st for:", filename)
		}

		itm.st = t_st
		itm.f = t_f
		itm.is = itm.st.GetCollection("int_string")
		if itm.is == nil {
			log.Fatal("Nil Collection name int_string")
		}
		itm.re = regexp.MustCompile("(http[s]?://)([^/]*)")
		return itm
	}
}

type ByteAble interface {
	ToBa() []byte
}

func toBa(st interface{}) []byte {
	var bs []byte
	switch v := st.(type) {
	case int:
		bs = []byte(strconv.Itoa(v))
	case string:
		bs = []byte(v)
	case []byte:
		bs = v
	case ByteAble:
		bs = v.ToBa()
	default:
		log.Fatalf("Unknown Type in toBa Conversion %T\n", st)
	}

	return bs
}
func (v Url) ToBa() []byte {
	return []byte(v.Url())
}
func (st *DkStore) SetAny(key, val interface{}) {
	key_bs := toBa(key)
	val_bs := toBa(val)
	st.is.Set(key_bs, val_bs)
}

func (st *DkStore) GetAny(key interface{}) []byte {
	key_bs := toBa(key)
	ret_val, err := st.is.Get(key_bs)
	check(err)
	return ret_val
}
func (st *DkStore) Delete(key interface{}) bool {
	key_bs := toBa(key)
	was_deleted, err := st.is.Delete(key_bs)
	check(err)
	return was_deleted
}

func (st *DkStore) Exist(key interface{}) bool {
	key_bs := toBa(key)
	val, _ := st.is.GetItem(key_bs, false)
	return val != nil
}
func (st *DkStore) GetString(key interface{}) string {
	return string(st.GetAny(key))
}
func (st *DkStore) GetInt(key interface{}) int {
	val, err := strconv.Atoi(st.GetString(key))
	check(err)
	return val
}

func (st *DkStore) GetIntKeys() (ret_chan chan int) {
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
func (st *DkStore) GetStringKeys() (ret_chan chan string) {
	ret_chan = make(chan string)
	go func() {

		for ba := range st.GetAnyKeys() {
			key := string(ba)
			ret_chan <- key
		}
		close(ret_chan)
	}()
	return ret_chan
}
func (st *DkStore) GetStringKeysArray(max_items int) (ret_arr []string) {
	ret_arr = make([]string, 0, max_items)
	for _, ba := range st.GetAnyKeysArray(max_items) {
		key := string(ba)
		ret_arr = append(ret_arr, key)
	}
	return ret_arr
}

func (st *DkStore) GetAnyValues() (ret_chan chan []byte) {
	ret_chan = make(chan []byte)
	go func() {
		st.is.VisitItemsAscend([]byte(string(0)), true, func(i *gkvlite.Item) bool {
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
func (st *DkStore) GetAnyKeysArray(max_items int) (ret_array [][]byte) {
	ret_array = make([][]byte, 0, max_items)
	tmp_chan := make(chan []byte)
	go func() {
		cnt := 0
		min_itm, err := st.is.MinItem(true)
		check(err)
		st.is.VisitItemsAscend(min_itm.Key, true, func(i *gkvlite.Item) bool {
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
func (st *DkStore) GetAnyKeys() (ret_chan chan []byte) {
	ret_chan = make(chan []byte)
	go func() {
		min_itm, err := st.is.MinItem(true)
		check(err)
		if min_itm != nil {
			st.is.VisitItemsAscend(min_itm.Key, true, func(i *gkvlite.Item) bool {
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

func (st *DkStore) GetMissing(max_items int, refr *TokenChan) (ret_map map[string]struct{}) {
	ret_map = make(map[string]struct{})

	// we will allow up to max_same of the same basename to go through
	// This is so we can fetch several things fromt he same host at once
	// But not too many!
	// It wants to be slightly more than the number of tokens we allow
	// for each basename
	max_same := max_items / 10 // fetch at least 10 domains worth to work with
	// We don't need to check the token as
	// we only re-search when we have done a full list search
	check_tk := false
	url_chan := make(chan string)
	go func() {
		cnt := 0
		min_itm, err := st.is.MinItem(true)
		check(err)
		if min_itm == nil {
			return
		}
		unique_array := make(map[string]int)
		if st == nil {
			log.Fatal("Invalid Stoer")
		}
		if st.is == nil {
			log.Fatal("Invalid collection")
		}
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

			//tmp_base := GetBase(tmp_val)
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
			url_chan <- tmp_val
			// keep going until we have got n items
			return cnt < max_items
		}
		st.is.VisitItemsAscend(min_itm.Key, true, explore_func)
		close(url_chan)
	}()
	for tmp_val := range url_chan {
		ret_map[tmp_val] = struct{}{}
	}

	return ret_map
}

func (st *DkStore) Size() int {
	size := 0
	if st.is == nil {
		// The collection has not been set up yet
		return 0
	}
	min_itm, err := st.is.MinItem(true)
	check(err)
	if min_itm == nil {
		//mpty list if no minimum
		return size
	}
	st.is.VisitItemsAscend(min_itm.Key, true, func(i *gkvlite.Item) bool {
		// This visitor callback will be invoked with every item
		// If we want to stop visiting, return false;
		// otherwise return true to keep visiting.
		size++
		//mmediatly stops for performance
		return false
	})
	return size
}

func (st *DkStore) Count() int {
	if st.is == nil {
		// The collection has not been set up yet
		return 0
	}
	numItems, _, err := st.is.GetTotals()
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

func (st *DkStore) PrintWorkload() {
	min_itm, err := st.is.MinItem(true)
	check(err)
	unique_array := make(map[string]int)
	s := Spinner{scaler: 1000}
	var cnt int
	st.is.VisitItemsAscend(min_itm.Key, true, func(i *gkvlite.Item) bool {
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
