package grab

import (
	"log"
	"os"
	"sync"
	"time"
)

var CsLockDebug = false

// A Concurrent safe file structure
// Trades off some memory use for speed
type ConcSafe struct {
	lk         *sync.Mutex
	write_lock *sync.Mutex
	osf        *os.File

	write_queue chan TxRequest
	read_queue  chan TxRequest
	read_ret    chan ReadReturn

	// These structures allow multiple outstanding writes
	tagLk        *sync.Mutex
	out_writes   map[int64]struct{}
	tagBroadcast chan struct{}
}
type TxRequest struct {
	B   []byte
	Off int64
}
type ReadReturn struct {
	N   int
	Err error
}

func (cs *ConcSafe) init() {
	cs.lk = new(sync.Mutex)
	cs.write_lock = new(sync.Mutex)
	cs.write_queue = make(chan TxRequest)
	cs.read_queue = make(chan TxRequest)
	cs.read_ret = make(chan ReadReturn)
	cs.out_writes = make(map[int64]struct{})
	cs.tagBroadcast = make(chan struct{})
	cs.tagLk = new(sync.Mutex)
}
func NewConcSafe(ff string) (cs *ConcSafe, err error) {
	cs = new(ConcSafe)
	cs.init()
	cs.osf, err = os.Create(ff)
	go cs.worker()
	return
}
func OpenConcSafe(ff string) (cs *ConcSafe, err error) {
	cs = new(ConcSafe)
	cs.init()

	cs.osf, err = os.OpenFile(ff, os.O_RDWR, 0600)
	if cs.osf == nil {
		log.Fatal("Nil file descriptor when reading:", ff)
	}

	go cs.worker()
	return
}
func (cs ConcSafe) ccWriteAt(b []byte, off int64) (n int, err error) {
	return cs.osf.WriteAt(b, off)
}

var CsWriteQueue = true
var CsReadQueue = true

func (cs ConcSafe) WriteAt(b []byte, off int64) (n int, err error) {
	if CsWriteQueue {
		qreq := TxRequest{B: b, Off: off}
		// For performance I think this could be a RWLock
		// i.e. there could be several queuing for access to the queue
		// But still allow the exclusive Lock to take place
		// when we wasnt to flush the queue
		cs.write_lock.Lock()
		cs.write_queue <- qreq
		cs.write_lock.Unlock()
		n = len(b)
		err = nil
	} else {
		n, err = cs.ccWriteAt(b, off)
	}
	return
}
func (cs ConcSafe) ccReadAt(b []byte, off int64) (n int, err error) {
	cs.lk.Lock()
	defer cs.lk.Unlock()
	return cs.osf.ReadAt(b, off)
}
func (cs ConcSafe) ReadAt(b []byte, off int64) (n int, err error) {

	if CsReadQueue {
		read_request := TxRequest{B: b, Off: off}
		cs.read_queue <- read_request
		rret := <-cs.read_ret
		n = rret.N
		err = rret.Err
	} else {
		n, err = cs.ccReadAt(b, off)
	}
	return
}
func (cs ConcSafe) ccTruncate(size int64) error {
	cs.lk.Lock()
	defer cs.lk.Unlock()
	return cs.osf.Truncate(size)
}
func (cs ConcSafe) Truncate(size int64) error {
	return cs.ccTruncate(size)
}
func (cs ConcSafe) ccStat() (os.FileInfo, error) {
	cs.lk.Lock()
	defer cs.lk.Unlock()
	return cs.osf.Stat()
}
func (cs ConcSafe) Stat() (os.FileInfo, error) {
	if CsLockDebug {
		log.Println("Stat Write Lock")
	}
	cs.write_lock.Lock()
	if CsLockDebug {
		log.Println("Stat Write Locked")
	}
	defer cs.write_lock.Unlock()
	cs.flushWriteCache()
	cs.flushWriteQ()
	return cs.ccStat()
}
func (cs ConcSafe) ccSync() error {
	cs.lk.Lock()
	defer cs.lk.Unlock()
	return cs.osf.Sync()
}
func (cs ConcSafe) Sync() error {

	if CsLockDebug {
		log.Println("Sync Write Lock")
	}
	cs.write_lock.Lock()
	if CsLockDebug {
		log.Println("Sync Write Locked")
	}
	defer cs.write_lock.Unlock()
	cs.flushWriteCache()
	cs.flushWriteQ()
	return cs.ccSync()
}
func (cs ConcSafe) Close() error {
	if CsLockDebug {
		log.Println("Close Write Lock")
	}
	cs.write_lock.Lock()
	if CsLockDebug {
		log.Println("Close Write Locked")
	}
	cs.flushWriteCache()
	cs.flushWriteQ()
	close(cs.read_queue)
	close(cs.write_queue)
	cs.write_lock.Unlock()
	return cs.osf.Close()
}
func (cs ConcSafe) flushWriteCache() {
	cs.tagLk.Lock()
	// Make sure any cached writes have completed
	cs.waitAllTag()
	// Keep hold of the tag until the end to stop any more from starting
	defer cs.tagLk.Unlock()
}
func (cs ConcSafe) flushWriteQ() {
	cs.lk.Lock()
	defer cs.lk.Unlock()

	for {
		select {
		case wr_req, ok := <-cs.write_queue:
			// Drain the write queue
			if !ok {
				return
			}
			b := wr_req.B
			off := wr_req.Off
			cs.ccWriteDelayed(b, off)
		default:
			// The write queue must be empty
			return
		}
	}
}

// To reduce the workload we store a reference to the
const TagWidth = 16 // Ech tag holds 16 bytes
const l2TagWidth = 4

func getTag(b []byte, off int64) (r_len int, r_off int64) {
	len_b := len(b)
	r_off = off >> l2TagWidth
	r_len = len_b >> l2TagWidth
	return
}

// Returns true if the described transaction exists in the cache
func (cs ConcSafe) testTag(b []byte, off int64) bool {
	r_len, r_off := getTag(b, off)
	for r_len++; r_len > 0; r_len-- {
		_, ok := cs.out_writes[r_off]
		if ok {
			return true
		}
		r_off++
	}
	return false
}

func (cs *ConcSafe) sendBroadcast() {
	cs.tagLk.Lock()
	close(cs.tagBroadcast)
	cs.tagBroadcast = make(chan struct{})
	cs.tagLk.Unlock()
}

func (cs *ConcSafe) waitBroadcast() {
	select {
	case <-cs.tagBroadcast:
	case <-time.After(time.Second):
		// This is the timeout case
		// worst case scenario
		log.Fatal("Timeout case used in conc_safe file access")
	}
}

// Wait until the tag no longer exists
// Allowing the assumption most transactions will not overlap
// There should be very few of these active
// Therefore we can do it the easy way
func (cs *ConcSafe) waitTag(b []byte, off int64) {
	// This MUST happen outside this function
	//cs.tagLk.Lock()
	for cs.testTag(b, off) {
		// Wait for the instruction to wake up and check if we have been woken
		cs.tagLk.Unlock()
		cs.waitBroadcast()
		cs.tagLk.Lock()
	}
	//cs.tagLk.Unlock()
}
func (cs *ConcSafe) waitAllTag() {
	// This MUST happen outside this function
	//cs.tagLk.Lock()
	for len(cs.out_writes) > 0 {
		// Wait for the instruction to wake up and check if we have been woken
		cs.tagLk.Unlock()
		cs.waitBroadcast()
		cs.tagLk.Lock()
	}
	//cs.tagLk.Unlock()
}

// Take a file access request and set the appropriate bytes in the tag
func (cs ConcSafe) setTag(b []byte, off int64) {
	// Translate the incomming into out reduced for the tag format
	r_len, r_off := getTag(b, off)

	// if the length is 0 we still want to do 1
	// access. Otherise do one for every line remenining
	for r_len++; r_len > 0; r_len-- {
		_, ok := cs.out_writes[r_off]
		if ok {
			log.Fatal("Accessing an offset that already exists", r_off)
		}
		cs.out_writes[r_off] = struct{}{}
		r_off++
	}
}
func (cs ConcSafe) clearTag(b []byte, off int64) {
	// Translate the incomming into out reduced for the tag format
	r_len, r_off := getTag(b, off)

	// if the length is 0 we still want to do 1
	// access. Otherise do one for every line remenining
	for r_len++; r_len > 0; r_len-- {
		_, ok := cs.out_writes[r_off]
		if !ok {
			log.Fatal("Clearing an offset that doesn't exist", r_off)
		}
		delete(cs.out_writes, r_off)
		r_off++
	}
}

// Send the write to the FS and when it is done update the tag
func (cs *ConcSafe) delayedWrite(b []byte, off int64) {
	// Send the write to the file systm
	cs.ccWriteAt(b, off)

	// Now the write has completed, update the map to remove it
	cs.tagLk.Lock()
	cs.clearTag(b, off)
	cs.tagLk.Unlock()

	// Tell any sleeping Tags to wake up
	// and check if we have cleared the reason for their waiting
	cs.sendBroadcast()
}

// Equivalent to normal write function
// but using the delayed write mechanism
func (cs *ConcSafe) ccWriteDelayed(b []byte, off int64) (out_len int) {
	// Get the lock on the delayed write structure
	cs.tagLk.Lock()
	// If the entry already exists
	if cs.testTag(b, off) {
		// wait until it doesn't
		cs.waitTag(b, off)
	}
	// Now it doesn't exist, we can do the write
	// So update the struct with the write
	cs.setTag(b, off)
	// While we have the lock, get the current length of the pending queue
	out_len = len(cs.out_writes)
	// No more use for the lock
	cs.tagLk.Unlock()
	// Do the write at your leisure
	go cs.delayedWrite(b, off)
	return
}

func (cs *ConcSafe) worker() {
	parallel_mode := false
	var out_len int
	var re_count int
	for {
		if out_len > 1024 {
			time.Sleep(10 * time.Millisecond)
			cs.tagLk.Lock()
			out_len = len(cs.out_writes)
			cs.tagLk.Unlock()
			re_count++
			if re_count > 100 {
				log.Fatal("conc_safe retry count error")
			}
		} else {
			re_count = 0
			select {
			case wr_req := <-cs.write_queue:
				// Get the lock to the file system
				b := wr_req.B
				off := wr_req.Off

				if parallel_mode {
					//cs.lk.Lock()
					// This is returning the number of oustanding/delayed write transactions
					out_len = cs.ccWriteDelayed(b, off)
					//cs.lk.Unlock()
				} else {
					cs.lk.Lock()
					cs.ccWriteAt(b, off)
					cs.lk.Unlock()
				}
			case rd_req := <-cs.read_queue:
				b := rd_req.B
				off := rd_req.Off

				if CsLockDebug {
					log.Println("Attempting Read Worker Lock")
				}
				cs.write_lock.Lock()
				if CsLockDebug {
					log.Println("Read worker Lock achieved")
				}
				// Make sure any pending writes are out of the queue
				// And into the write cache
				cs.flushWriteQ()
				// Safe to add things to the queue as logically they will
				// now occur after this transaction
				cs.write_lock.Unlock()

				// Wait until the requested read is not in the write cache
				// Other things could be in the cache, as long as this isn't
				cs.tagLk.Lock()
				cs.waitTag(b, off)
				cs.tagLk.Unlock()

				// Perform the read
				n, err := cs.ccReadAt(b, off)
				rret := ReadReturn{N: n, Err: err}
				cs.read_ret <- rret
			}
		}
	}
}
