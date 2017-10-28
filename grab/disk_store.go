package grab

import (
	"log"
	"os"
	"strconv"

	"github.com/cbehopkins/gkvlite"
)

// UseConcSafe - Should we use (our own) the Concurrent Safe
// file system.
// Probably not, but useful if we suspect problems
var UseConcSafe = false

// DkStFileIf Any type that implements this interface
// can be used by us
type DkStFileIf interface {
	ReadAt([]byte, int64) (int, error)
	WriteAt([]byte, int64) (int, error)
	Truncate(size int64) error
	Sync() error
	Close() error
	Stat() (os.FileInfo, error)
}

// DkStore is the disk store
type DkStore struct {
	st *gkvlite.Store
	f  DkStFileIf
}

// Flush out the store to file
// Does not then sync
func (ds *DkStore) Flush() {
	err := ds.st.Flush()
	check(err)
}

// Sync the outstanding changes to disk
func (ds *DkStore) Sync() {
	err := ds.f.Sync()
	check(err)
}

// Close will close off the store
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
	// If the file does not exist already
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
func openFile(filename string) (tF DkStFileIf, err error) {
	if UseConcSafe {
		tF, err = OpenConcSafe(filename)
	} else {
		tF, err = os.OpenFile(filename, os.O_RDWR, 0600)
	}
	return
}
func (ds *DkStore) openStore(filename string, overwrite bool) {
	if _, err := os.Stat(filename); overwrite || os.IsNotExist(err) {
		// Doesn't exist? Happy days, create it
		ds.f, ds.st = newStore(filename)
	} else {
		var err error
		ds.f, err = openFile(filename)
		check(err)

		ds.st, err = gkvlite.NewStore(ds.f)
		check(err)
		if ds.st == nil {
			log.Fatal("Nil t_st for:", filename)
		}
	}
	return
}

// ByteAble is a type that
// can be turned into a byte array
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
