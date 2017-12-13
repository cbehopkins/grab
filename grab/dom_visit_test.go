package grab

import (
	"log"
	"testing"
)

func TestDv0(t *testing.T) {
	src := []string{
		"http://www.google.com",
		"http://www.google.com/",
		"https://www.google.com/bob/categories/150",
	}
	dv := NewDomVisit("")
	for _, urlS := range src {
		url := NewURL(urlS)
		if !dv.GoodURL(url) {
			log.Fatal("url should be good and isn't", url, urlS)
		}
		dv.VisitedA(urlS)
	}
	log.Println("Added everything")
	for _, urlS := range src {
		url := NewURL(urlS)
		if !dv.VisitedQ(url.URL()) {
			log.Fatal("A good domain does not exist", urlS)
		}
	}
}
