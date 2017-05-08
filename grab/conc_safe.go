package grab

import (
	"log"
	"os"
	"sync"
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
}
type TxRequest struct {
	B   []byte
	Off int64
}
type ReadReturn struct {
	N   int
	Err error
}

func NewConcSafe(ff string) (cs *ConcSafe, err error) {
	cs = new(ConcSafe)
	cs.lk = new(sync.Mutex)
	cs.write_lock = new(sync.Mutex)
	cs.write_queue = make(chan TxRequest)
	cs.read_queue = make(chan TxRequest)
	cs.read_ret = make(chan ReadReturn)

	cs.osf, err = os.Create(ff)
	go cs.worker()
	return
}
func OpenConcSafe(ff string) (cs *ConcSafe, err error) {
	cs = new(ConcSafe)
	cs.lk = new(sync.Mutex)
	cs.write_lock = new(sync.Mutex)
	cs.write_queue = make(chan TxRequest, 2048)
	cs.read_queue = make(chan TxRequest)
	cs.read_ret = make(chan ReadReturn)

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
	cs.flushWriteQ()
	close(cs.read_queue)
	close(cs.write_queue)
	cs.write_lock.Unlock()
	return cs.osf.Close()
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
			cs.ccWriteAt(wr_req.B, wr_req.Off)
		default:
			// The write queue must be empty
			return
		}
	}
}
func (cs ConcSafe) worker() {
	for {
		select {
		case wr_req := <-cs.write_queue:
			cs.lk.Lock()
			cs.ccWriteAt(wr_req.B, wr_req.Off)
			cs.lk.Unlock()
		case rd_req := <-cs.read_queue:
			if CsLockDebug {
				log.Println("Attempting Read Worker Lock")
			}
			cs.write_lock.Lock()
			if CsLockDebug {
				log.Println("Read worker Lock achieved")
			}
			cs.flushWriteQ()
			n, err := cs.ccReadAt(rd_req.B, rd_req.Off)
			// Don't allow writes again until read complete
			cs.write_lock.Unlock()
			rret := ReadReturn{N: n, Err: err}
			cs.read_ret <- rret
		}
	}
}
