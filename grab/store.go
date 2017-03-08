package grab

import (
	"fmt"
	"log"
	"sync"
)

// This is an implementation of an arbitrarily large queue
// Arbitratily large in that you can just keep writing to it
// and the queue will grow in length to accomodate this
// Yes I know this can consume all system memory, but so can
// any algorithm that fundamentally 1 worker can create N more items
type UrlStore struct {
	sync.Mutex
	data          *[]Url
	PushChannel   UrlChannel
	PopChannel    UrlChannel
	enforce_order bool
	InCount       int
	OutCount      int
	data_valid    bool
	// This is really important!
	// the pointers can go to twice the size of the queue
	// This allows a full queue to use the full capacity
	// an empty queue is when rd == wr
	// a full queue is distinquished when rd%cap==wr%cap
	read_pointer  int
	write_pointer int
	debug         bool
}

func NewUrlStore(in_if_arr ...interface{}) *UrlStore {
	itm := new(UrlStore)
	tmp_store := make([]Url, 8)
	itm.data = &tmp_store
	//itm.data = make([]Url, 8)
	itm.PopChannel = make(chan Url, 2)
	for _, in_if := range in_if_arr {
		switch in_if.(type) {
		case chan Url:
			itm.PushChannel = in_if.(chan Url)
		case UrlChannel:
			itm.PushChannel = in_if.(UrlChannel)
		case bool:
			// A bool type is controlling our ordering attribute
			itm.enforce_order = in_if.(bool)
		case nil:
			//Nothing to do
		default:
			fmt.Printf("Type is %T\n", in_if)
		}
	}

	if itm.PushChannel == nil {
		itm.PushChannel = make(chan Url, 2)
	}

	go itm.urlWorker()
	return itm
}
func (us *UrlStore) SetDebug() {
	us.debug = true
}
func (us UrlStore) data_cap() int {
	return cap(*us.data)
}
func (us UrlStore) DataCap() int {
        return us.data_cap()
}

func (us UrlStore) data_items() int {
	q_cap := us.DataCap()
	rd_norm := us.read_pointer % q_cap
	wr_norm := us.write_pointer % q_cap

	if us.read_pointer == us.write_pointer {
		return 0
	} else if us.read_pointer%q_cap == us.write_pointer%q_cap {
		return q_cap
	} else if rd_norm < wr_norm {
		// Easy case, the write pointer is larer
		return wr_norm - rd_norm
	} else if wr_norm < rd_norm {
		return us.data_cap() - rd_norm + wr_norm
	}
	return 0
}
func (us UrlStore) data_free() int {
	return us.data_cap() - us.data_items()
}
func (us UrlStore) grow_store(a,b,c,d int) {
	old_store := *us.data
        store_capacity := us.DataCap()
        new_store := make([]Url, store_capacity<<1)

	if a!=b {
		copy(new_store, old_store[a:b])
	}
	if (c!=d) {
		copy(new_store[b-a:b],old_store[c:d])
	}
	*us.data = new_store
}
func (us UrlStore) DataGrow(a,b,c,d int) {
	us.grow_store(a,b,c,d)
}
func (us *UrlStore) grow_data() {

	// Now we have it copy the new data into it
	// Now we can make some assumptions. The only reasong we are here
	// must be because the queue is FULL
	// The only problem is EXACTLY where is the valid data
	if us.read_pointer == us.write_pointer {
		log.Fatal("Stupid sod you're telling me to grow an empty queue")
	}
	q_cap := us.DataCap()
	rd_norm := us.read_pointer % q_cap
	wr_norm := us.write_pointer % q_cap
	if rd_norm != wr_norm {
		log.Fatal("You are running grow with a none full queue")
	} else if us.read_pointer > us.write_pointer {
		if us.read_pointer > q_cap {
			// There is valid data from the read pointer
			// to the end of the buffer
			// then from the start of the buffer to the write pointer
			// 5,6,7,x,x,x,2,3,4
			//
			if us.debug {
				fmt.Println("Rp>Wp")
			}
			us.grow_store(rd_norm,q_cap,0,wr_norm)
		} else {
			// Data starts at read pointer and goes on until the write pointer
			// How is this possible?
			// x,x,x,2,3,4,5,6,7,x,x
			if us.debug {
				fmt.Println("Rp>Wp, Weird case")

				fmt.Printf("Cap=%d\nWP=%d,RP=%d\nnWP=%d,nRP=%d\n", q_cap, us.write_pointer, us.read_pointer, wr_norm, rd_norm)
			}
			us.grow_store(rd_norm,wr_norm,0,0)
		}
	} else /*write pointer is larger*/ {
		if us.write_pointer >= q_cap {
			// There is data that starts at the read pointer and goes to the end of the buffer
			// then data that start at the buffer and goes to the write pointer
			// i.e. we wrote in some data that wrapped over into the upper region
			// and read some of that
			// so the old data is at the
			// 5,6,7,x,x,x,2,3,4
			if us.debug {
				fmt.Println("Complex Wp>Rp")
			}
			us.grow_store(rd_norm,q_cap,0,wr_norm)
		} else {
			// there is data that starts at the read pointer and goes on until the write pointer
			// i.e. we wrote some data in (and didn't wrap) and read some of that
			// x,x,x,2,3,4,5,6,7,x,x
			if us.debug {
				fmt.Println("Simple Wp>Rp,")
			}
			us.grow_store(rd_norm,wr_norm,0,0)
		}
	}
	us.read_pointer = 0
	us.write_pointer = q_cap
}

// Now we do
func (us *UrlStore) add_store(item Url) {
	// This is the code we are aiming for:
	// location, us.Fifo = us.Fifo.AddHead(us)
	// data_store := *us.data
	// data_store[location] = item
	// us.data_valid = true


	// add an item into the main store
	if us.data_free() > 0 {
		location := us.write_pointer % us.data_cap()
		//fmt.Printf("Write to %d, WP=%d,cap=%d\n", location, us.write_pointer, us.data_cap())
		data_store := *us.data
		data_store[location] = item
		us.write_pointer++
		if us.write_pointer >= (us.data_cap() << 1) {
			us.write_pointer = 0
		}
		//fmt.Printf("Now its %d, WP=%d,cap=%d\n", location, us.write_pointer, us.data_cap())
		//fmt.Printf("There are %d items\n", us.data_items())
	} else if us.data_items() < 0 {
		log.Fatal("Error negative queue size")
	} else {
		us.grow_data()
		// GrowStore modifies the underlying structure, so pick it up again
		data_store := *us.data

		data_store[us.write_pointer] = item
		us.write_pointer++
	}
	us.data_valid = true
}
// Temp Func
func (us UrlStore) get_tail() int {
	location := us.read_pointer % us.data_cap()
	return location
}
func (us UrlStore) peek_store() Url {
	data_store := *us.data
	if us.data_items() > 0 {
		location := us.get_tail()
		value := data_store[location]
		return value
	}
	return ""
}

func (us *UrlStore) advance_store() bool {
	//if us.FifoP.FifoItems() >0 {
	// us.FifoP = us.FifoP.AdvanceTail()
	// us.data_valid = (us.FifoP.DataItems()>0)
	// return true
	// } else {
	// return false
	//
	if us.data_items() > 0 {
		us.read_pointer++
		if us.read_pointer >= (us.data_cap() << 1) {
			us.read_pointer = 0
		}
		if us.data_items() == 0 {
			// If the store empties then say so
			us.data_valid = false
		}
		return true
	} else {
		return false
	}
}

func (us *UrlStore) urlWorker() {
	var tmp_out_chan chan Url
	in_chan := us.PushChannel
	var input_channel_closed bool
	var tmp_val Url
	for {
		if us.data_valid {
			tmp_out_chan = us.PopChannel
			tmp_val = us.peek_store() // Peek at what the current value is
		} else {
			tmp_out_chan = nil
			if input_channel_closed == true {
				//fmt.Println("Channel found closed")
				close(us.PopChannel)
				//return
			}
		}
		select {
		// If we can read from the input channel
		case ind, ok := <-in_chan:
			if ok {
				if us.debug {
					fmt.Println("Queueing URL :", ind)
				}
				us.Lock()
				us.InCount++
				us.Unlock()

				// Put the data into the main store
				us.add_store(ind)
				if us.data_items() == 0 {
					log.Fatal("Data failed to update")
				}

				// activate the pop channel
				tmp_out_chan = us.PopChannel
				us.data_valid = true
			} else {
				//chanel is closed
				//fmt.Println("in_chan Channel is closed")
				in_chan = nil
				input_channel_closed = true
			}
		// If it is possible to write to us.PopChannel
		case tmp_out_chan <- tmp_val:
			us.Lock()
			us.OutCount++
			us.Unlock()
			if us.debug {
				fmt.Println("DeQueueing URL", tmp_val)
			}

			if !us.advance_store() {
				//fmt.Println("Shutting down receiver")
				close(us.PopChannel)
				return
			}

		}
	}
}

func (us UrlStore) Close() {
	//fmt.Println("Closing Push")
	close(us.PushChannel)
}
func (us UrlStore) Push(itm Url) {
	us.PushChannel <- itm
	if us.debug {
		fmt.Println("Push URL:", itm)
	}
}

func (us UrlStore) Pop() (itm Url, ok bool) {
	itm, ok = <-us.PopChannel
	if us.debug {
		fmt.Println("Popped URL:", itm)
	}
	return
}
func (us UrlStore) InputCount() int {
	return us.InCount + len(us.PushChannel)
}
func (us UrlStore) OutputCount() int {
	return us.OutCount + len(us.PopChannel)
}
