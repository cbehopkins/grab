package grab

import (
	"fmt"
	"log"
)

// FifoProto - is the prototype for a fifo
// contains the structures that allow us to operate on an array
// of generic tyoe
// by out standard interface to that tyupe
type FifoProto struct {
	rp  int
	wp  int
	cap int
}

// FifoInt Is the interface a type needs to implement
// to support a fifo
type FifoInt interface {
	// Moodify self to be of newCapacity
	// copying in a through to b
	// then append c through d
	// this allows you to take a wrap-around style fifo
	// and in one move create a new data store with contiguous data
	DataResize(a, b, c, d, newCapacity int)
}

// SetCap - Set the capacity of the fifo
func (fp *FifoProto) SetCap(iniCap int) {
	fp.cap = iniCap
}

func (fp FifoProto) String() string {
	return fmt.Sprintf("Rd=%d,Wp=%d\n", fp.rp, fp.wp)
}
func (fp FifoProto) fifoItems() int {
	// The number of items in the fifo
	qCap := fp.cap
	rdNorm := fp.rp % qCap
	wrNorm := fp.wp % qCap

	if fp.rp == fp.wp {
		return 0
	} else if fp.rp%qCap == fp.wp%qCap {
		return qCap
	} else if rdNorm < wrNorm {
		// Easy case, the write pointer is larer
		return wrNorm - rdNorm
	} else if wrNorm < rdNorm {
		return qCap - rdNorm + wrNorm
	}
	return 0
}

// DataItems Return the count of the number of items in the fifo
func (fp FifoProto) DataItems() int {
	return fp.fifoItems()
}

// DataValid returns true if there is valid data in the fifo
func (fp FifoProto) DataValid() bool {
	// Canonically this is the point of the function
	//return (fp.fifo_items() >0)
	return fp.rp != fp.wp
}

func (fp FifoProto) fifoFree() int {
	// Simply the capacity of the data store - the number of items in it
	return fp.cap - fp.fifoItems()
}

// AddHead Add an item to the fifo
// Note this only operates on the pointers
// Returns the location one should use in the array
// and a new proto structure
func (fp FifoProto) AddHead(inter FifoInt) (int, FifoProto) {
	// add an item into the main store
	location := fp.wp % fp.cap
	if fp.fifoFree() > 0 {
		fp.wp++
		if fp.wp >= (fp.cap << 1) {
			fp.wp = 0
		}
	} else if fp.fifoItems() < 0 {
		log.Fatal("Error negative queue size")
	} else {
		fp = fp.GrowStore(inter)
		location = fp.wp
		fp.wp++
		//fmt.Println("After Grow:",fp)
	}
	return location, fp
}

// GetTail returns the tail of the current fifo
func (fp FifoProto) GetTail() (int, bool) {
	qCap := fp.cap
	return fp.rp % qCap, fp.rp != fp.wp
}

// AdvanceTail moves the tail on by one
func (fp FifoProto) AdvanceTail(inter FifoInt) FifoProto {
	fp.rp++
	// TBD put check in here to look for it the queue is underutilized
	// and to optionally shrink it if so.
	if fp.rp >= (fp.cap << 1) {
		fp.rp = 0
	}

	return fp
}

// GrowStore Grows the store specified
// Returns a new proto for that store
func (fp FifoProto) GrowStore(inter FifoInt) FifoProto {
	// Now we have it copy the new data into it
	// Now we can make some assumptions. The only reasong we are here
	// must be because the queue is FULL
	// The only problem is EXACTLY where is the valid data
	if fp.rp == fp.wp {
		log.Fatal("Stupid sod you're telling me to grow an empty queue")
	}
	if fp.rp > fp.wp {
		fp.resize(inter, fp.rp)
	} else {
		fp.resize(inter, fp.wp)
	}
	return fp
}

func (fp *FifoProto) resize(inter FifoInt, point int) {
	qCap := fp.cap
	rdNorm := fp.rp % qCap
	wrNorm := fp.wp % qCap
	newCap := qCap << 1
	if rdNorm != wrNorm {
		log.Fatal("You are running grow with a none full queue")
	}
	if point >= qCap {
		inter.DataResize(rdNorm, qCap, 0, wrNorm, newCap)
	} else {
		inter.DataResize(rdNorm, wrNorm, 0, 0, newCap)
	}
	fp.rp = 0
	fp.wp = qCap
	fp.cap = newCap
}
