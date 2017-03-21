package grab

import (
	"os"
	"strconv"
	"log"
	"github.com/steveyen/gkvlite"
)

// Disk Store of int to string map
type DkStore struct {
	st *gkvlite.Store
	is *gkvlite.Collection
	f *os.File
}

func (ds *DkStore) Flush() {
	ds.st.Flush()
	ds.f.Sync()
}
func (ds *DkStore) Close () {
	ds.Flush()
	ds.f.Close()
}
func newStore(filename string) (*os.File, *gkvlite.Store) {
	// Remember to close the file later
	f, err := os.Create(filename)
	check(err)
	itm := new(gkvlite.Store)
	var ok error
	itm, ok = gkvlite.NewStore(f)
	check(ok)
	return f,itm
}
func NewDkStore(filename string) *DkStore {
	itm := new(DkStore)
	f, st := newStore(filename)
	itm.st = st
	itm.f = f
	itm.is = itm.st.SetCollection("int_string", nil)
	return itm
}

func (st *DkStore) Compact() {
	// Copy st.st to a new store
	// and update accordingly
	new_filename := "/tmp/new.gkvlite"
	f, err := os.Create(new_filename)
	check(err)
	new_st, err := st.st.CopyTo(f, 20000)
	check(err)
	st.st = new_st

}
type ByteAble interface {
	ToBa () []byte
}
func toBa (st interface{}) []byte {
	var bs []byte
	switch v := st.(type) {
	case int:
		bs = []byte(strconv.Itoa(v))
	case string:
		bs = []byte(v)
	case []byte:
		bs = v
	case ByteAble :
		bs = v.ToBa()
	default:
		log.Fatalf("Unknown Type in toBa Conversion %T\n",st)
	}

	return bs
}
func (v Url) ToBa () []byte {
	return []byte(v)
}
func (st *DkStore) SetAny(key,val interface{}) {
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
func (st *DkStore) Delete (key interface{}) bool {
        key_bs := toBa(key)
	was_deleted,err := st.is.Delete(key_bs)
	check(err)
	return was_deleted
}

func (st *DkStore) Exist (key interface{}) bool {
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
func (st *DkStore) GetString (key interface{}) string {
	return string(st.GetAny(key))
}
func (st *DkStore) GetInt (key interface{}) int {
	val,err :=  strconv.Atoi(st.GetString(key))
	check(err)
	return val
}

func (st *DkStore) GetIntKeys() (ret_chan chan int) {
	ret_chan = make(chan int)
	go func() {

	for ba := range st.GetAnyKeys() {
		key,err :=  strconv.Atoi(string(ba))
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
		key :=  string(ba)
		ret_chan <- key 
	}
	close(ret_chan)
	}()
       return ret_chan

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
func (st *DkStore) GetAnyKeys() (ret_chan chan []byte) {
        ret_chan = make(chan []byte)
        go func() {
		min_itm, err := st.is.MinItem(true)
		check(err)
                st.is.VisitItemsAscend(min_itm.Key, true, func(i *gkvlite.Item) bool {
                        // This visitor callback will be invoked with every item
                        // If we want to stop visiting, return false;
                        // otherwise return true to keep visiting.
                        ret_chan <- i.Key
                        return true
                })
                close(ret_chan)
        }()
        return ret_chan
}
func (st *DkStore) Size() int {
		size:=0
		if st.is == nil {
			// The collection has not been set up yet
			return size
		}
		min_itm, err := st.is.MinItem(true)
                check(err)
		if min_itm == nil {
			// empty list if no minimum
			return size
		}
                st.is.VisitItemsAscend(min_itm.Key, true, func(i *gkvlite.Item) bool {
                        // This visitor callback will be invoked with every item
                        // If we want to stop visiting, return false;
                        // otherwise return true to keep visiting.
			size++
			// Immediatly stops for performance
                        return false
                })
		return size
}

