package grab

import (
	"log"
	"os"
	"sync"
	"time"
)

var csLockDebug = false

// ConcSafe inplements a Concurrent safe file structure
// Trades off some memory use for speed
type ConcSafe struct {
	lk        *sync.Mutex
	writeLock *sync.Mutex
	osf       *os.File

	writeQueue chan TxRequest
	readQueue  chan TxRequest
	readRet    chan ReadReturn

	// These structures allow multiple outstanding writes
	tagLk        *sync.Mutex
	outWrites    map[int64]struct{}
	tagBroadcast chan struct{}
}

// TxRequest - what do you want to transmit
type TxRequest struct {
	B   []byte
	Off int64
}

// ReadReturn - What comes back fwith a read
type ReadReturn struct {
	N   int
	Err error
}

func (cs *ConcSafe) init() {
	cs.lk = new(sync.Mutex)
	cs.writeLock = new(sync.Mutex)
	cs.writeQueue = make(chan TxRequest)
	cs.readQueue = make(chan TxRequest)
	cs.readRet = make(chan ReadReturn)
	cs.outWrites = make(map[int64]struct{})
	cs.tagBroadcast = make(chan struct{})
	cs.tagLk = new(sync.Mutex)
}

// NewConcSafe - a new instance of a concsafe
// pass it a filename and you get a file
func NewConcSafe(ff string) (cs *ConcSafe, err error) {
	cs = new(ConcSafe)
	cs.init()
	cs.osf, err = os.Create(ff)
	go cs.worker()
	return
}

// OpenConcSafe same as NewConcSafe
// but for existing files
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

var csWriteQueue = true
var csReadQueue = true

// WriteAt implements the standard WriteAt interface speify where you want to write int he file
func (cs ConcSafe) WriteAt(b []byte, off int64) (n int, err error) {
	if csWriteQueue {
		qreq := TxRequest{B: b, Off: off}
		// For performance I think this could be a RWLock
		// i.e. there could be several queuing for access to the queue
		// But still allow the exclusive Lock to take place
		// when we wasnt to flush the queue
		cs.writeLock.Lock()
		cs.writeQueue <- qreq
		cs.writeLock.Unlock()
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

// ReadAt implements the Standard ReadAt interface - specify where you want to write in file
func (cs ConcSafe) ReadAt(b []byte, off int64) (n int, err error) {

	if csReadQueue {
		readRequest := TxRequest{B: b, Off: off}
		cs.readQueue <- readRequest
		rret := <-cs.readRet
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

// Truncate implements the  Standard Truncate interface
// specify how long you want to truncate the file to
func (cs ConcSafe) Truncate(size int64) error {
	return cs.ccTruncate(size)
}
func (cs ConcSafe) ccStat() (os.FileInfo, error) {
	cs.lk.Lock()
	defer cs.lk.Unlock()
	return cs.osf.Stat()
}

// Stat implements the Standard os.FileInfo statistics
func (cs ConcSafe) Stat() (os.FileInfo, error) {
	if csLockDebug {
		log.Println("Stat Write Lock")
	}
	cs.writeLock.Lock()
	if csLockDebug {
		log.Println("Stat Write Locked")
	}
	defer cs.writeLock.Unlock()
	cs.flushWriteCache()
	cs.flushWriteQ()
	return cs.ccStat()
}
func (cs ConcSafe) ccSync() error {
	cs.lk.Lock()
	defer cs.lk.Unlock()
	return cs.osf.Sync()
}

// Sync implements the Standard file sync interface
// sync changes to disk
func (cs ConcSafe) Sync() error {

	if csLockDebug {
		log.Println("Sync Write Lock")
	}
	cs.writeLock.Lock()
	if csLockDebug {
		log.Println("Sync Write Locked")
	}
	defer cs.writeLock.Unlock()
	cs.flushWriteCache()
	cs.flushWriteQ()
	return cs.ccSync()
}

// Close an open file
func (cs ConcSafe) Close() error {
	if csLockDebug {
		log.Println("Close Write Lock")
	}
	cs.writeLock.Lock()
	if csLockDebug {
		log.Println("Close Write Locked")
	}
	cs.flushWriteCache()
	cs.flushWriteQ()
	close(cs.readQueue)
	close(cs.writeQueue)
	cs.writeLock.Unlock()
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
		case wrReq, ok := <-cs.writeQueue:
			// Drain the write queue
			if !ok {
				return
			}
			b := wrReq.B
			off := wrReq.Off
			cs.ccWriteDelayed(b, off)
		default:
			// The write queue must be empty
			return
		}
	}
}

// TagWidth specifies number of bytes in a tag
// To reduce the workload we store a reference to the
const TagWidth = 16 // Ech tag holds 16 bytes
// l2TagWidth number of bits needed for tag width
const l2TagWidth = 4

func getTag(b []byte, off int64) (rLen int, rOff int64) {
	lenB := len(b)
	rOff = off >> l2TagWidth
	rLen = lenB >> l2TagWidth
	return
}

// Returns true if the described transaction exists in the cache
func (cs ConcSafe) testTag(b []byte, off int64) bool {
	rLen, rOff := getTag(b, off)
	for rLen++; rLen > 0; rLen-- {
		_, ok := cs.outWrites[rOff]
		if ok {
			return true
		}
		rOff++
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
	for len(cs.outWrites) > 0 {
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
	rLen, rOff := getTag(b, off)

	// if the length is 0 we still want to do 1
	// access. Otherise do one for every line remenining
	for rLen++; rLen > 0; rLen-- {
		_, ok := cs.outWrites[rOff]
		if ok {
			log.Fatal("Accessing an offset that already exists", rOff)
		}
		cs.outWrites[rOff] = struct{}{}
		rOff++
	}
}
func (cs ConcSafe) clearTag(b []byte, off int64) {
	// Translate the incomming into out reduced for the tag format
	rLen, rOff := getTag(b, off)

	// if the length is 0 we still want to do 1
	// access. Otherise do one for every line remenining
	for rLen++; rLen > 0; rLen-- {
		_, ok := cs.outWrites[rOff]
		if !ok {
			log.Fatal("Clearing an offset that doesn't exist", rOff)
		}
		delete(cs.outWrites, rOff)
		rOff++
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
func (cs *ConcSafe) ccWriteDelayed(b []byte, off int64) (outLen int) {
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
	outLen = len(cs.outWrites)
	// No more use for the lock
	cs.tagLk.Unlock()
	// Do the write at your leisure
	go cs.delayedWrite(b, off)
	return
}

func (cs *ConcSafe) worker() {
	parallelMode := false
	var outLen int
	var reCount int
	for {
		if outLen > 1024 {
			time.Sleep(10 * time.Millisecond)
			cs.tagLk.Lock()
			outLen = len(cs.outWrites)
			cs.tagLk.Unlock()
			reCount++
			if reCount > 100 {
				log.Fatal("conc_safe retry count error")
			}
		} else {
			reCount = 0
			select {
			case wrReq := <-cs.writeQueue:
				// Get the lock to the file system
				b := wrReq.B
				off := wrReq.Off

				if parallelMode {
					//cs.lk.Lock()
					// This is returning the number of oustanding/delayed write transactions
					outLen = cs.ccWriteDelayed(b, off)
					//cs.lk.Unlock()
				} else {
					cs.lk.Lock()
					cs.ccWriteAt(b, off)
					cs.lk.Unlock()
				}
			case rdReq := <-cs.readQueue:
				b := rdReq.B
				off := rdReq.Off

				if csLockDebug {
					log.Println("Attempting Read Worker Lock")
				}
				cs.writeLock.Lock()
				if csLockDebug {
					log.Println("Read worker Lock achieved")
				}
				// Make sure any pending writes are out of the queue
				// And into the write cache
				cs.flushWriteQ()
				// Safe to add things to the queue as logically they will
				// now occur after this transaction
				cs.writeLock.Unlock()

				// Wait until the requested read is not in the write cache
				// Other things could be in the cache, as long as this isn't
				cs.tagLk.Lock()
				cs.waitTag(b, off)
				cs.tagLk.Unlock()

				// Perform the read
				n, err := cs.ccReadAt(b, off)
				rret := ReadReturn{N: n, Err: err}
				cs.readRet <- rret
			}
		}
	}
}
