package grab

import (
	"fmt"
	"log"
)

type FifoProto struct {
	rp int
	wp int
	cap int
}
func (fp *FifoProto) SetCap (ini_cap int) {
	fp.cap = ini_cap
}
type FifoInt interface {
	// In order to function as a fifoable structure
	// an origionating data type must implement the following methods
	DataCap() int // The capacity of the underlying structure
	DataGrow(a, b, c, d int)
}

func (fp FifoProto) String() string {
	return fmt.Sprintf("Rd=%d,Wp=%d\n", fp.rp, fp.wp)
}
func (fp FifoProto) fifo_items() int {
	// The number of items in the fifo
	q_cap := fp.cap
	rd_norm := fp.rp % q_cap
	wr_norm := fp.wp % q_cap

	if fp.rp == fp.wp {
		return 0
	} else if fp.rp%q_cap == fp.wp%q_cap {
		return q_cap
	} else if rd_norm < wr_norm {
		// Easy case, the write pointer is larer
		return wr_norm - rd_norm
	} else if wr_norm < rd_norm {
		return q_cap - rd_norm + wr_norm
	}
	return 0
}
func (fp FifoProto) DataItems() int {
	return fp.fifo_items()
}
func (fp FifoProto) DataValid() bool {
	// Canonically this is the point of the function
	//return (fp.fifo_items() >0)
	return (fp.rp != fp.wp)
}

func (fp FifoProto) fifo_free() int {
	// Simply the capacity of the data store - the number of items in it
	return fp.cap - fp.fifo_items()
}

func (fp FifoProto) AddHead(inter FifoInt) (int, FifoProto) {
	// add an item into the main store
	location := fp.wp % fp.cap
	if fp.fifo_free() > 0 {
		fp.wp++
		if fp.wp >= (fp.cap << 1) {
			fp.wp = 0
		}
	} else if fp.fifo_items() < 0 {
		log.Fatal("Error negative queue size")
	} else {
		fp = fp.GrowStore(inter)
		location = fp.wp
		fp.wp++
		//fmt.Println("After Grow:",fp)
	}
	return location, fp
}
func (fp FifoProto) GetTail() int {
	q_cap := fp.cap
	return fp.rp % q_cap
}
func (fp FifoProto) AdvanceTail() FifoProto {
	fp.rp++
	if fp.rp >= (fp.cap << 1) {
		fp.rp = 0
	}

	return fp
}

func (fp FifoProto) GrowStore(inter FifoInt) FifoProto {
	debug := false
	// Now we have it copy the new data into it
	// Now we can make some assumptions. The only reasong we are here
	// must be because the queue is FULL
	// The only problem is EXACTLY where is the valid data
	if fp.rp == fp.wp {
		log.Fatal("Stupid sod you're telling me to grow an empty queue")
	}
	q_cap := fp.cap
	rd_norm := fp.rp % q_cap
	wr_norm := fp.wp % q_cap
	if rd_norm != wr_norm {
		log.Fatal("You are running grow with a none full queue")
	} else if fp.rp > fp.wp {
		if fp.rp > q_cap {
			// There is valid data from the read pointer
			// to the end of the buffer
			// then from the start of the buffer to the write pointer
			// 5,6,7,x,x,x,2,3,4
			//
			if debug {
				log.Println("Rp>Wp")
			}
			inter.DataGrow(rd_norm, q_cap, 0, wr_norm)
		} else {
			// Data starts at read pointer and goes on until the write pointer
			// How is this possible?
			// x,x,x,2,3,4,5,6,7,x,x
			if debug {
				log.Println("Rp>Wp, Weird case")

				log.Printf("Cap=%d\nWP=%d,RP=%d\nnWP=%d,nRP=%d\n", q_cap, fp.wp, fp.rp, wr_norm, rd_norm)
			}
			inter.DataGrow(rd_norm, wr_norm, 0, 0)
		}
	} else /*write pointer is larger*/ {
		if fp.wp >= q_cap {
			// There is data that starts at the read pointer and goes to the end of the buffer
			// then data that start at the buffer and goes to the write pointer
			// i.e. we wrote in some data that wrapped over into the upper region
			// and read some of that
			// so the old data is at the
			// 5,6,7,x,x,x,2,3,4
			if debug {
				log.Println("Complex Wp>Rp")
			}
			inter.DataGrow(rd_norm, q_cap, 0, wr_norm)
		} else {
			// there is data that starts at the read pointer and goes on until the write pointer
			// i.e. we wrote some data in (and didn't wrap) and read some of that
			// x,x,x,2,3,4,5,6,7,x,x
			if debug {
				log.Println("Simple Wp>Rp,")
			}
			inter.DataGrow(rd_norm, wr_norm, 0, 0)
		}
	}
	fp.rp = 0
	fp.wp = q_cap
	fp.cap = q_cap<<1
	return fp
}
