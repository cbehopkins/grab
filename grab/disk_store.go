package grab

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/steveyen/gkvlite"
)

// Disk Store of int to string map
type DkStore struct {
	st *gkvlite.Store
	is *gkvlite.Collection
	f  *os.File
}

func (ds *DkStore) Flush() {
	ds.st.Flush()
}
func (ds *DkStore) Sync() {
	ds.f.Sync()
}
func (ds *DkStore) Close() {
	ds.st.Flush()
	//ds.st.Close()
	ds.f.Sync()
	ds.f.Close()
}
func newStore(filename string, overwrite bool) (*os.File, *gkvlite.Store) {
	var f *os.File
	st := new(gkvlite.Store)
	var err error
	// Remember to close the file later

	// If the file does not exiast already
	f, err = os.Create(filename)
	check(err)
	st, err = gkvlite.NewStore(f)
	check(err)

	return f, st
}
func NewDkStore(filename string, overwrite bool) *DkStore {
	itm := new(DkStore)
	if _, err := os.Stat(filename); overwrite || os.IsNotExist(err) {
		// Doesn't exist? Happy days, create it
		f, st := newStore(filename, overwrite)
		itm.st = st
		itm.f = f
		itm.is = itm.st.SetCollection("int_string", nil)
		return itm
	} else {
		t_itm := new(DkStore)
		new_filename := filename + "_tmp"
		err = os.Rename(filename, new_filename)
		check(err)

		t_f, err := os.Open(new_filename)
		check(err)
		f, err := os.Create(filename)
		check(err)

		t_st, err := gkvlite.NewStore(t_f)
		check(err)
		st, err := gkvlite.NewStore(f)
		check(err)

		itm.f = f
		itm.st = st
		t_itm.f = t_f
		t_itm.st = t_st

		itm.is = itm.st.SetCollection("int_string", nil)
		t_itm.is = t_itm.st.SetCollection("int_string", nil)
		for v := range t_itm.GetStringKeys() {
			itm.SetAny(v, "")
		}
		t_itm.Close()
		return itm

	}
}

func (st *DkStore) Compact(filename string) {
	// Copy st.st to a new store
	// and update accordingly
	new_filename := "/tmp/new.gkvlite"
	f, err := os.Create(new_filename)
	check(err)
	_, err = st.st.CopyTo(f, 20000)
	check(err)
	st.st.Close()
	f.Close()
	err = os.Rename(new_filename, filename)
	check(err)
	f, err = os.Open(filename)
	check(err)
	st.st, err = gkvlite.NewStore(f)
	check(err)
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
	return []byte(v)
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
	// Don't believe the documentation
	// GetItem() with withValue false does not work
	// In fact the testcase checks that val is valid with it false!
	val, _ := st.is.GetItem(key_bs, true)
	if val == nil {
		return false
	}
	return true
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

func (st *DkStore) GetMissing(max_items int, refr *TokenChan) (ret_array []string) {
	ret_array = make([]string, 0, max_items)
	tmp_chan := make(chan string)

	// we will allow up to max_same of the same basename to go through
	// This is so we can fetch several things fromt he same host at once
	// But not too many!
	// It wants to be slightly more than the number of tokens we allow
	// for each basename
	max_same := 20

	go func() {
		cnt := 0
		min_itm, err := st.is.MinItem(true)
		check(err)
		unique_array := make(map[string]int)
		st.is.VisitItemsAscend(min_itm.Key, true, func(i *gkvlite.Item) bool {
			// This visitor callback will be invoked with every item
			// If we want to stop visiting, return false;
			// otherwise return true to keep visiting.
			tmp_val := string(i.Key)
			// If the token for this is in use
			// Then it won't fetch this pass, so don't let it through
			ok := refr.UrlExist(tmp_val)
			if ok {
				// Keep going, look for something not in the map
				return true
			}
			// We could have a bunch in the same base domain

			tmp_base := GetBase(tmp_val)
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
			tmp_chan <- tmp_val
			// keep going until we have got n items
			return cnt < max_items
		})
		close(tmp_chan)
	}()
	var done_some bool
	for v := range tmp_chan {
		done_some = true
		ret_array = append(ret_array, v)
	}
	if !done_some {
		log.Println("Warning no new items")
		<-time.After(10 * time.Second)
	}
	return ret_array
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
	//numItems, _, err := st.is.GetTotals()
	//check(err)
	//return int(numItems)
}
