package grab

import (
	"fmt"
	"log"
	//"sync"
)

// URLStore store the Urls together
// This is an implementation of an arbitrarily large queue
// Arbitratily large in that you can just keep writing to it
// and the queue will grow in length to accomodate this
// Yes I know this can consume all system memory, but so can
// any algorithm that fundamentally 1 worker can create N more items
type URLStore struct {
	//sync.Mutex
	data        *[]URL
	PushChannel URLChannel
	PopChannel  URLChannel
	inCount     int
	outCount    int
	fifoP       FifoProto
	debug       bool
}

// NewURLStore create a new store for urls
func NewURLStore(inIfArr ...interface{}) *URLStore {
	itm := new(URLStore)
	iniSize := 8
	tmpStore := make([]URL, iniSize)
	itm.data = &tmpStore
	itm.fifoP.SetCap(iniSize)
	itm.PopChannel = make(chan URL, 2)
	for _, inIf := range inIfArr {
		switch inIf.(type) {
		case chan URL:
			itm.PushChannel = inIf.(chan URL)
		case URLChannel:
			itm.PushChannel = inIf.(URLChannel)
		case nil:
			//Nothing to do
		default:
			fmt.Printf("Type is %T\n", inIf)
		}
	}

	if itm.PushChannel == nil {
		itm.PushChannel = make(chan URL, 2)
	}

	go itm.urlWorker()
	return itm
}
func (us URLStore) String() string {
	var retStr string
	retStr = fmt.Sprintf("***Data Store***\n%v\n", *us.data)
	retStr += fmt.Sprintf("Capacity:%d\n", us.dataCap())
	retStr += us.fifoP.String()
	return retStr
}

// SetDebug Turn on debug mode
func (us *URLStore) SetDebug() {
	us.debug = true
}
func (us URLStore) dataCap() int {
	return cap(*us.data)
}
func (us URLStore) resizeStore(a, b, c, d, newCapacity int) {
	oldStore := *us.data
	if ((b - a) + (d - c)) > newCapacity {
		log.Fatal("New store is too small for existing capacity")
	}
	newStore := make([]URL, newCapacity)

	if a != b {
		copy(newStore, oldStore[a:b])
	}
	if c != d {
		copy(newStore[b-a:b], oldStore[c:d])
	}
	*us.data = newStore
}

// DataResize - To satisfty the FifoInt interface you need this:
// Copy the data ionto a contiguis form in a new buffer of specified size
func (us URLStore) DataResize(a, b, c, d, newCapacity int) {
	us.resizeStore(a, b, c, d, newCapacity)
}

// Add an item to the store
func (us *URLStore) addStore(item URL) {
	var location int

	// To what location should we store the data we are going to add
	// and update the modified pointers after this
	location, us.fifoP = us.fifoP.AddHead(us)
	dataStore := *us.data
	dataStore[location] = item
	if us.debug {
		fmt.Printf("Adding %v\n%v\nLoc:%d\n", item, us, location)
	}
}

// Peek at the item in the store without modifying it
func (us URLStore) peekStore() URL {
	dataStore := *us.data
	location, ok := us.fifoP.GetTail()
	if ok {
		value := dataStore[location]
		return value
	}
	return NewURL("")
}

func (us *URLStore) advanceStore() bool {
	if us.fifoP.DataValid() {
		us.fifoP = us.fifoP.AdvanceTail(us)
		return true
	}
	return false
}

func (us *URLStore) urlWorker() {
	var tmpOutChan chan URL
	inChan := us.PushChannel
	var inputChannelClosed bool
	var tmpVal URL
	for {
		if us.fifoP.DataValid() {
			tmpOutChan = us.PopChannel
			tmpVal = us.peekStore() // Peek at what the current value is
		} else {
			tmpOutChan = nil
			if inputChannelClosed == true {
				close(us.PopChannel)
			}
		}
		select {
		// If we can read from the input channel
		case ind, ok := <-inChan:
			if ok {
				if us.debug {
					fmt.Println("Queueing URL :", ind)
				}
				//us.Lock()
				us.inCount++
				//us.Unlock()

				// Put the data into the main store
				us.addStore(ind)

				// activate the pop channel
				tmpOutChan = us.PopChannel
			} else {
				//chanel is closed
				//fmt.Println("in_chan Channel is closed")
				inChan = nil
				inputChannelClosed = true
			}
		// If it is possible to write to us.PopChannel
		case tmpOutChan <- tmpVal:
			//us.Lock()
			us.outCount++
			//us.Unlock()
			if us.debug {
				fmt.Println("DeQueueing URL", tmpVal)
			}

			if !us.advanceStore() {
				//fmt.Println("Shutting down receiver")
				close(us.PopChannel)
				return
			}

		}
	}
}

// Close down the store
func (us URLStore) Close() {
	if us.debug {
		fmt.Println("Closing Push")
	}
	close(us.PushChannel)
}

// Push an item onto the store
func (us URLStore) Push(itm URL) {
	us.PushChannel <- itm
	if us.debug {
		fmt.Println("Push URL:", itm)
	}
}

// Pop an item off the sotre
func (us URLStore) Pop() (itm URL, ok bool) {
	itm, ok = <-us.PopChannel
	if us.debug {
		fmt.Println("Popped URL:", itm)
	}
	return
}

// InputCount How many things have we received?
func (us URLStore) InputCount() int {
	return us.inCount + len(us.PushChannel)
}

// OutputCount return How many things re have sent
func (us URLStore) OutputCount() int {
	return us.outCount + len(us.PopChannel)
}

// Count how many items are we holding
func (us URLStore) Count() int {
	return us.InputCount() - us.OutputCount()
}
