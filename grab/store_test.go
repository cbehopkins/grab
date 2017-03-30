package grab

import (
	"log"
	"strconv"
	"testing"
)

func TestCall4(t *testing.T) {

	url_store := NewUrlStore()
	// Receive from chUrls and store in a temporary buffer
	url := "URL"
	num_tests := 4
	for url_int := 0; url_int < num_tests; url_int++ {
		tmp_url := url + strconv.Itoa(url_int)
		url_store.Push(NewUrl(tmp_url))
	}
	url_store.Close()
	rx_count := 0
	for url, ok := url_store.Pop(); ok; url, ok = url_store.Pop() {
		rx_count++
		if false {
			log.Println("Got:", url)
		}
	}
	if rx_count != num_tests {
		log.Fatal("Insufficient entries:", rx_count)
	}
	log.Println("All Done")
}
func TestCall16(t *testing.T) {

	url_store := NewUrlStore()
	// Receive from chUrls and store in a temporary buffer
	url := "URL"
	num_tests := 16
	for url_int := 0; url_int < num_tests; url_int++ {
		tmp_url := url + strconv.Itoa(url_int)
		url_store.Push(NewUrl(tmp_url))
	}
	url_store.Close()
	rx_count := 0
	for url, ok := url_store.Pop(); ok; url, ok = url_store.Pop() {
		tmp_url := NewUrl("URL" + strconv.Itoa(rx_count))
		if tmp_url != url {
			log.Fatalf("Bad Got, Rxd:%s, Exp:%s\n", url, tmp_url)
		} else {
			//log.Printf("Good Got:%s,%s\n", url, tmp_url)
		}
		rx_count++
		if false {
			log.Println("Got:", url)
		}
	}
	if rx_count != num_tests {
		log.Fatal("Insufficient entries:", rx_count)
	}
	log.Println("All Done")
}
func TestChan(t *testing.T) {

	tst := make(chan Url)
	url_store := NewUrlStore(tst)
	// Receive from chUrls and store in a temporary buffer
	url := "URL"
	for url_int := 0; url_int < 20; url_int++ {
		tmp_url := url + strconv.Itoa(url_int)
		url_store.PushChannel <- NewUrl(tmp_url)
	}
	url_store.Close()
	rx_count := 0
	for url := range url_store.PopChannel {
		rx_count++
		if false {
			log.Println("Got:", url)
		}
	}
	if rx_count != 20 {
		log.Fatal("Insufficient entries:", rx_count)
	}
	log.Println("All Done")
}
func TestChanP0(t *testing.T) {
	for i := 1; i < 2049; i++ {
		i_local := i
		name := strconv.Itoa(i)
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			test_num(i_local)
		})
	}

}
func test_num(cnt int) {
	tst := make(chan Url)
	url_store := NewUrlStore(tst)
	//url_store.SetDebug()
	// Receive from chUrls and store in a temporary buffer
	url := "URL"
	go func() {
		for url_int := 0; url_int < cnt; url_int++ {
			tmp_url := url + strconv.Itoa(url_int)
			url_store.PushChannel <- NewUrl(tmp_url)
		}
		url_store.Close()
	}()
	rx_count := 0
	for url := range url_store.PopChannel {
		rx_count++
		if false {
			log.Println("Got:", url)
		}
	}
	if rx_count != cnt {
		log.Fatal("Insufficient entries:", rx_count)
	}
	log.Println("All Done:", cnt)
}
func TestCallE(t *testing.T) {

	url_store := NewUrlStore(true)
	// Receive from chUrls and store in a temporary buffer
	url := "URL"
	num_tests := 2048
	for url_int := 0; url_int < num_tests; url_int++ {
		tmp_url := url + strconv.Itoa(url_int)
		url_store.Push(NewUrl(tmp_url))
	}
	url_store.Close()
	rx_count := 0
	for url, ok := url_store.Pop(); ok; url, ok = url_store.Pop() {
		tmp_url := NewUrl("URL" + strconv.Itoa(rx_count))
		if tmp_url != url {
			log.Fatalf("Bad Got:%s,%s\n", url, tmp_url)
		} else {
			//log.Printf("Good Got:%s,%s\n", url, tmp_url)
		}
		rx_count++
	}
	if rx_count != num_tests {
		log.Fatal("Insufficient entries:", rx_count)
	}
	log.Println("All Done")
}

func TestChanE(t *testing.T) {

	tst := make(chan Url)
	url_store := NewUrlStore(tst, true)
	// Receive from chUrls and store in a temporary buffer
	url := "URL"
	for url_int := 0; url_int < 20; url_int++ {
		tmp_url := url + strconv.Itoa(url_int)
		url_store.PushChannel <- NewUrl(tmp_url)
	}
	url_store.Close()
	rx_count := 0
	for url := range url_store.PopChannel {
		tmp_url := NewUrl("URL" + strconv.Itoa(rx_count))
		if tmp_url != url {
			log.Fatal("Got:", url, tmp_url)
		}
		rx_count++
	}
	if rx_count != 20 {
		log.Fatal("Insufficient entries:", rx_count)
	}
	log.Println("All Done")
}
