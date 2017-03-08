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
	//read_pointer  int
	//write_pointer int
	FifoP		FifoProto
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

// Now we do
func (us *UrlStore) add_store(item Url) {
	var location int
	var ffp FifoProto
	 location, ffp = us.FifoP.AddHead(us)
	assign us.FifoP = ffp
	 data_store := *us.data
	 data_store[location] = item
	 us.data_valid = true
}
func (us UrlStore) peek_store() Url {
	data_store := *us.data
	if us.FifoP.DataItems(us) > 0 {
		location := us.FifoP.GetTail()
		value := data_store[location]
		return value
	}
	return ""
}

func (us *UrlStore) advance_store() bool {
	if us.FifoP.FifoItems() >0 {
	 	us.FifoP = us.FifoP.AdvanceTail()
	 	us.data_valid = (us.FifoP.DataItems()>0)
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
				if us.FifoP.DataItems() == 0 {
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
