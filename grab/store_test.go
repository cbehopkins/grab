package grab

import (
	"log"
	"strconv"
	"testing"
)

func TestCall(t *testing.T) {

	url_store := NewUrlStore()
	// Receive from chUrls and store in a temporary buffer
	url := "URL"
	num_tests := 20
	for url_int := 0; url_int < num_tests; url_int++ {
		tmp_url := url + strconv.Itoa(url_int)
		url_store.Add(Url(tmp_url))
	}
	url_store.Close()
	rx_count := 0
	for url, ok := url_store.Pop(); ok; url, ok = url_store.Pop() {
		rx_count++
		log.Println("Got:", url)
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
		url_store.PushChannel <- Url(tmp_url)
	}
	url_store.Close()
	rx_count := 0
	for url := range url_store.PopChannel {
		rx_count++
		log.Println("Got:", url)
	}
	if rx_count != 20 {
		log.Fatal("Insufficient entries:", rx_count)
	}
	log.Println("All Done")
}

func TestCallE(t *testing.T) {

	url_store := NewUrlStore(true)
	// Receive from chUrls and store in a temporary buffer
	url := "URL"
	num_tests := 20
	for url_int := 0; url_int < num_tests; url_int++ {
		tmp_url := url + strconv.Itoa(url_int)
		url_store.Add(Url(tmp_url))
	}
	url_store.Close()
	rx_count := 0
	for url, ok := url_store.Pop(); ok; url, ok = url_store.Pop() {
		tmp_url := "URL" + Url(strconv.Itoa(rx_count))
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
		url_store.PushChannel <- Url(tmp_url)
	}
	url_store.Close()
	rx_count := 0
	for url := range url_store.PopChannel {
		tmp_url := "URL" + Url(strconv.Itoa(rx_count))
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
