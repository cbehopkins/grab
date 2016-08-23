package grab

import (
	"fmt"
	"testing"
	"strings"
)


func BasicTest (t *testing.T)) {


        url_store := NewUrlStore()
        // Receive from chUrls and store in a temporary buffer
	url := "URL"
        for url_int := 0; url_int <2048; url_int++{
		tmp_url = url+strings.ItoA(url_int)
                url_store.Add(temp_url)
        }
        for url, ok := url_store.Pop(); ok; url, ok = url_store.Pop() {
		fmt.Println("Got:",url)
	}
	fmt.Println("All Done")
}
