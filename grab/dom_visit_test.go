package grab

import (
	"log"
	"testing"
)

func TestDv0(t *testing.T) {
	if useTestParallel {
		t.Parallel()
	}
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
func TestDv1(t *testing.T) {
	if useTestParallel {
		t.Parallel()
	}
	src := []string{
		"http://fred.google.com",
		"http://bob.google.com/",
	}
	fail := []string{
		"http://google.com",
		"http://steve.google.com/",
	}
	dv := NewDomVisit("")
	for _, urlS := range src {
		url := NewURL(urlS)
		if !dv.GoodURL(url) {
			log.Fatal("url should be good and isn't", url, urlS)
		}
		dv.VisitedA(urlS)
	}
	log.Println("Added everything", dv)
	for _, urlS := range src {
		url := NewURL(urlS)
		if !dv.VisitedQ(url.URL()) {
			log.Fatal("A good domain does not exist", urlS)
		}
	}
	log.Println("Checked good")
	for _, urlS := range fail {
		url := NewURL(urlS)
		if dv.VisitedQ(url.URL()) {
			log.Fatal("A bad domain does exist", urlS)
		}
	}
}

func TestDv2(t *testing.T) {
	if useTestParallel {
		t.Parallel()
	}
	src := []string{
		"http://*.google.com",
		"http://google.com/",
	}
	good := []string{
		"http://steve.google.com",
		"http://bob.google.com/",
	}
	fail := []string{
		"http://gogle.com",
	}
	dv := NewDomVisit("")
	for _, urlS := range src {
		url := NewURL(urlS)
		if !dv.GoodURL(url) {
			log.Fatal("url should be good and isn't", url, urlS)
		}
		dv.VisitedA(urlS)
	}
	log.Println("Added everything", dv)
	for _, urlS := range good {
		url := NewURL(urlS)
		if !dv.VisitedQ(url.URL()) {
			log.Fatal("A good domain does not exist", urlS)
		}
	}
	log.Println("Checked good")
	for _, urlS := range fail {
		url := NewURL(urlS)
		if dv.VisitedQ(url.URL()) {
			log.Fatal("A bad domain does exist", urlS)
		}
	}
}
