package grab

import (
	"fmt"
	"log"
	//"sync"
)

// This is an implementation of an arbitrarily large queue
// Arbitratily large in that you can just keep writing to it
// and the queue will grow in length to accomodate this
// Yes I know this can consume all system memory, but so can
// any algorithm that fundamentally 1 worker can create N more items
type UrlStore struct {
	//sync.Mutex
	data        *[]Url
	PushChannel UrlChannel
	PopChannel  UrlChannel
	inCount     int
	outCount    int
	fifoP       FifoProto
	debug       bool
}

func NewUrlStore(in_if_arr ...interface{}) *UrlStore {
	itm := new(UrlStore)
	ini_size := 8
	tmp_store := make([]Url, ini_size)
	itm.data = &tmp_store
	itm.fifoP.SetCap(ini_size)
	itm.PopChannel = make(chan Url, 2)
	for _, in_if := range in_if_arr {
		switch in_if.(type) {
		case chan Url:
			itm.PushChannel = in_if.(chan Url)
		case UrlChannel:
			itm.PushChannel = in_if.(UrlChannel)
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
func (us UrlStore) String() string {
	var ret_str string
	ret_str = fmt.Sprintf("***Data Store***\n%v\n", *us.data)
	ret_str += fmt.Sprintf("Capacity:%d\n", us.data_cap())
	ret_str += us.fifoP.String()
	return ret_str
}
func (us *UrlStore) SetDebug() {
	us.debug = true
}
func (us UrlStore) data_cap() int {
	return cap(*us.data)
}
func (us UrlStore) resize_store(a, b, c, d, new_capacity int) {
	old_store := *us.data
	if ((b - a) + (d - c)) > new_capacity {
		log.Fatal("New store is too small for existing capacity")
	}
	new_store := make([]Url, new_capacity)

	if a != b {
		copy(new_store, old_store[a:b])
	}
	if c != d {
		copy(new_store[b-a:b], old_store[c:d])
	}
	*us.data = new_store
}

// To satisfty the FifoInt interface you need this:
func (us UrlStore) DataResize(a, b, c, d, new_capacity int) {
	us.resize_store(a, b, c, d, new_capacity)
}

// Add an item to the store
func (us *UrlStore) add_store(item Url) {
	var location int

	// To what location should we store the data we are going to add
	// and update the modified pointers after this
	location, us.fifoP = us.fifoP.AddHead(us)
	data_store := *us.data
	data_store[location] = item
	if us.debug {
		fmt.Printf("Adding %v\n%v\nLoc:%d\n", item, us, location)
	}
}

// Peek at the item in the store without modifying it
func (us UrlStore) peek_store() Url {
	data_store := *us.data
	location, ok := us.fifoP.GetTail()
	if ok {
		value := data_store[location]
		return value
	} else {
		return NewUrl("")
	}
}

func (us *UrlStore) advance_store() bool {
	if us.fifoP.DataValid() {
		us.fifoP = us.fifoP.AdvanceTail(us)
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
		if us.fifoP.DataValid() {
			tmp_out_chan = us.PopChannel
			tmp_val = us.peek_store() // Peek at what the current value is
		} else {
			tmp_out_chan = nil
			if input_channel_closed == true {
				close(us.PopChannel)
			}
		}
		select {
		// If we can read from the input channel
		case ind, ok := <-in_chan:
			if ok {
				if us.debug {
					fmt.Println("Queueing URL :", ind)
				}
				//us.Lock()
				us.inCount++
				//us.Unlock()

				// Put the data into the main store
				us.add_store(ind)

				// activate the pop channel
				tmp_out_chan = us.PopChannel
			} else {
				//chanel is closed
				//fmt.Println("in_chan Channel is closed")
				in_chan = nil
				input_channel_closed = true
			}
		// If it is possible to write to us.PopChannel
		case tmp_out_chan <- tmp_val:
			//us.Lock()
			us.outCount++
			//us.Unlock()
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
	if us.debug {
		fmt.Println("Closing Push")
	}
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
	return us.inCount + len(us.PushChannel)
}
func (us UrlStore) OutputCount() int {
	return us.outCount + len(us.PopChannel)
}

func (us UrlStore) Count() int {
	return us.InputCount() - us.OutputCount()
}
