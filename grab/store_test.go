package grab

import (
	"fmt"
	"strconv"
	"testing"
)

func TestCall(t *testing.T) {

	url_store := NewUrlStore(nil)
	// Receive from chUrls and store in a temporary buffer
	url := "URL"
	for url_int := 0; url_int < 20; url_int++ {
		tmp_url := url + strconv.Itoa(url_int)
		url_store.Add(Url(tmp_url))
	}
	url_store.Close()
	for url, ok := url_store.Pop(); ok; url, ok = url_store.Pop() {
		fmt.Println("Got:", url)
	}
	fmt.Println("All Done")
}

func TestChan(t *testing.T) {

	tst := make(chan Url)
	url_store := NewUrlStore(tst)
	// Receive from chUrls and store in a temporary buffer
	url := "URL"
	for url_int := 0; url_int < 20; url_int++ {
		tmp_url := url + strconv.Itoa(url_int)
		url_store.PushChannel <- Url(tmp_url)
	}
	url_store.Close()
	for url := range url_store.PopChannel {
		fmt.Println("Got:", url)
	}
	fmt.Println("All Done")
}
