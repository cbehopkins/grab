package grab

import (
	"log"
	"strconv"
	"testing"
)

func TestCall4(t *testing.T) {

	urlStore := NewURLStore()
	// Receive from chURLs and store in a temporary buffer
	url := "URL"
	numTests := 4
	for urlInt := 0; urlInt < numTests; urlInt++ {
		tmpURL := url + strconv.Itoa(urlInt)
		urlStore.Push(NewURL(tmpURL))
	}
	urlStore.Close()
	rxCount := 0
	for url, ok := urlStore.Pop(); ok; url, ok = urlStore.Pop() {
		rxCount++
		if false {
			log.Println("Got:", url)
		}
	}
	if rxCount != numTests {
		log.Fatal("Insufficient entries:", rxCount)
	}
	log.Println("All Done")
}
func TestCall16(t *testing.T) {

	urlStore := NewURLStore()
	// Receive from chURLs and store in a temporary buffer
	url := "URL"
	numTests := 16
	for urlInt := 0; urlInt < numTests; urlInt++ {
		tmpURL := url + strconv.Itoa(urlInt)
		urlStore.Push(NewURL(tmpURL))
	}
	urlStore.Close()
	rxCount := 0
	for url, ok := urlStore.Pop(); ok; url, ok = urlStore.Pop() {
		tmpURL := "URL" + strconv.Itoa(rxCount)
		if tmpURL != url.URL() {
			log.Fatalf("Bad Got, Rxd:\"%s\", Exp:\"%s\"\n", url.URL(), tmpURL)
		} else {
			//log.Printf("Good Got:%s,%s\n", url, tmpURL)
		}
		rxCount++
		if false {
			log.Println("Got:", url)
		}
	}
	if rxCount != numTests {
		log.Fatal("Insufficient entries:", rxCount)
	}
	log.Println("All Done")
}
func TestChan(t *testing.T) {

	tst := make(chan URL)
	urlStore := NewURLStore(tst)
	// Receive from chURLs and store in a temporary buffer
	url := "URL"
	for urlInt := 0; urlInt < 20; urlInt++ {
		tmpURL := url + strconv.Itoa(urlInt)
		urlStore.PushChannel <- NewURL(tmpURL)
	}
	urlStore.Close()
	rxCount := 0
	for url := range urlStore.PopChannel {
		rxCount++
		if false {
			log.Println("Got:", url)
		}
	}
	if rxCount != 20 {
		log.Fatal("Insufficient entries:", rxCount)
	}
	log.Println("All Done")
}
func TestChanP0(t *testing.T) {
	for i := 1; i < 2049; i++ {
		iLocal := i
		name := strconv.Itoa(i)
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testNum(iLocal)
		})
	}

}
func testNum(cnt int) {
	tst := make(chan URL)
	urlStore := NewURLStore(tst)
	//url_store.SetDebug()
	// Receive from chURLs and store in a temporary buffer
	url := "URL"
	go func() {
		for urlInt := 0; urlInt < cnt; urlInt++ {
			tmpURL := url + strconv.Itoa(urlInt)
			urlStore.PushChannel <- NewURL(tmpURL)
		}
		urlStore.Close()
	}()
	rxCount := 0
	for url := range urlStore.PopChannel {
		rxCount++
		if false {
			log.Println("Got:", url)
		}
	}
	if rxCount != cnt {
		log.Fatal("Insufficient entries:", rxCount)
	}
	log.Println("All Done:", cnt)
}
func TestCallE(t *testing.T) {

	urlStore := NewURLStore(true)
	// Receive from chURLs and store in a temporary buffer
	url := "URL"
	numTests := 2048
	for urlInt := 0; urlInt < numTests; urlInt++ {
		tmpURL := url + strconv.Itoa(urlInt)
		urlStore.Push(NewURL(tmpURL))
	}
	urlStore.Close()
	rxCount := 0
	for url, ok := urlStore.Pop(); ok; url, ok = urlStore.Pop() {
		tmpURL := "URL" + strconv.Itoa(rxCount)
		if tmpURL != url.URL() {
			log.Fatalf("Bad Got:%s,%s\n", url.URL(), tmpURL)
		} else {
			//log.Printf("Good Got:%s,%s\n", url, tmpURL)
		}
		rxCount++
	}
	if rxCount != numTests {
		log.Fatal("Insufficient entries:", rxCount)
	}
	log.Println("All Done")
}

func TestChanE(t *testing.T) {

	tst := make(chan URL)
	urlStore := NewURLStore(tst, true)
	// Receive from chURLs and store in a temporary buffer
	url := "URL"
	for urlInt := 0; urlInt < 20; urlInt++ {
		tmpURL := url + strconv.Itoa(urlInt)
		urlStore.PushChannel <- NewURL(tmpURL)
	}
	urlStore.Close()
	rxCount := 0
	for url := range urlStore.PopChannel {
		tmpURL := "URL" + strconv.Itoa(rxCount)
		if tmpURL != url.URL() {
			log.Fatal("Got:", url.URL(), tmpURL)
		}
		rxCount++
	}
	if rxCount != 20 {
		log.Fatal("Insufficient entries:", rxCount)
	}
	log.Println("All Done")
}
