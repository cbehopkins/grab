package grab

import (
	"fmt"
	"sync"
)
// This is a Lazy implementation of an arbitrarily large queue
// Arbitratily large in that you can just keep writing to it
// and the queue will grow in length to accomodate this
// Yes I know this can consume all system memory, but so can
// any algorithm that fundamentally 1 worker can create N more items
// We are Lazy because at the moment the algorithm on read/pop
// slices down the store, and on push a copy may happen
// This means a low load on the Memory allocation
// but a (potentiall) high load on copy
// More efficient would be to use a read and write pointer
// This that would significantly increase the complexity 
// of the code and we don't need the performance for our application
type UrlStore struct {
	sync.Mutex
	data          []Url
	PushChannel   UrlChannel
	PopChannel    UrlChannel
	enforce_order bool
	InCount       int
	OutCount      int
}

func NewUrlStore(in_if_arr ...interface{}) *UrlStore {
	itm := new(UrlStore)
	itm.data = make([]Url, 0, 1024)
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
func (us *UrlStore) urlWorker() {
	var tmp_chan chan Url
	in_chan := us.PushChannel
	var input_channel_closed bool
	backup_store := us.data
	var tmp_val Url
	for {
		if len(us.data) > 0 {
			tmp_chan = us.PopChannel
			if us.enforce_order {
				tmp_val = us.data[0]
			} else {
				tmp_val = us.data[len(us.data)-1]
			}
		} else {
			tmp_chan = nil
			if input_channel_closed == true {
				fmt.Println("Channel found closed")
				close(us.PopChannel)
				return
			}
		}
		select {
		case ind, ok := <-in_chan:
			if ok {
				//fmt.Println("Queueing URL :", ind)
				us.Lock()
				us.InCount++
				us.Unlock()
				if us.enforce_order {
					if len(us.data) < cap(us.data) {
						// Safe to use append here as it won't do the copy as there is capacity
						us.data = append(us.data, ind)
					} else {
						if cap(backup_store) > cap(us.data) {
							// here we copy into the backup store's data storage
							// the current data in the data store
							copy(backup_store, us.data[0:len(us.data)])
							// now stick the data on the end
							//backup_store = append(us.data, ind)
							//us.data = backup_store
							us.data = append(us.data, ind)
						} else {
							// This is the case where we need to grow the size of the store
							us.data = append(us.data, ind)
							backup_store = us.data
						}
					}
				} else {
					//in the normal case we just chuck the data on the end
					us.data = append(us.data, ind)
				}
				// activate the pop channel
				tmp_chan = us.PopChannel
			} else {
				//chanel is closed
				fmt.Println("in_chan Channel is closed")
				in_chan = nil
				input_channel_closed = true
			}
		// If it is possible to write to us.PopChannel
		case tmp_chan <- tmp_val:
			us.Lock()
			us.OutCount++
			us.Unlock()
			//fmt.Println("DeQueueing URL", tmp_val)
			if us.enforce_order {
				//copy(us.data[:len(us.data)-1],us.data[1:len(us.data)])
				//us.data = us.data[:len(us.data)-1]
				// Reduce the length and capacity
				us.data = us.data[1:len(us.data)]
			} else {
				// take one off the length without reducing the capacity
				us.data = us.data[:len(us.data)-1]
			}
		}
	}
}

func (us UrlStore) Close() {
	//fmt.Println("Closing Push")
	close(us.PushChannel)
}
func (us UrlStore) Add(itm Url) {
	us.PushChannel <- itm
}

func (us UrlStore) Pop() (itm Url, ok bool) {
	itm, ok = <-us.PopChannel
	//fmt.Println("Popped URL:", itm)
	return
}
func (us UrlStore) InputCount() int {
	return us.InCount + len(us.PushChannel)
}
func (us UrlStore) OutputCount() int {
	return us.OutCount + len(us.PopChannel)
}
