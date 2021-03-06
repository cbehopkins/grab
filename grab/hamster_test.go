package grab

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
)

type HamsterTestCase struct {
	DvA           []string
	GrabExpected  []string
	FetchExpected []string
	URLToVisit    string
}

func LoadHamsterTestCase(fn string) (htc HamsterTestCase, err error) {
	var f *os.File
	_, err = os.Stat(fn)

	if os.IsNotExist(err) {
		return
	}
	f, err = os.Open(fn)

	if err != nil {
		log.Printf("error opening directory map file: %T,%v\n", err, err)
		log.Println("Which is odd as:", os.IsNotExist(err), err)
		_, err = os.Stat(fn)
		log.Println("and:", os.IsNotExist(err), err)
		return
	}
	byteValue, err := ioutil.ReadAll(f)
	_ = f.Close()
	if err != nil {
		log.Fatalf("error loading file: %T,%v\n", err, err)
	}
	err = htc.FromJSON(byteValue)
	if err != nil {
		return
	}
	return
}
func (htc *HamsterTestCase) FromJSON(input []byte) (err error) {
	err = json.Unmarshal(input, htc)
	//fmt.Printf("Unmarshalling completed on:\n%v\nOutput:\n%v\n\n",input, htc)
	switch err {
	case nil:
	case io.EOF:
		err = nil
	default:
		log.Fatal("Unknown Error UnMarshalling Md5File:", err)
	}
	return
}
func TestValidSuffix(t *testing.T) {
	if useTestParallel {
		t.Parallel()
	}

	hm := NewHamster(false, false, false, false)
	type tv struct {
		testString string
		match      bool
	}
	testVals := []tv{
		{"this.mpg", true},
		{"https://3762493-480p.mp4", true},
		{"that.mp3", false},
	}
	for _, bob := range testVals {
		res := hm.validSuffix(NewURL(bob.testString), goodSuffix)
		if res != bob.match {
			log.Fatal("TestValidSuffix Fail on:", bob.testString)
		}
	}

}
func TestHam0(t *testing.T) {
	if useTestParallel {
		t.Parallel()
	}
	var promiscuous, allInteresting, printUrls, polite bool
	htc, err := LoadHamsterTestCase("htc2.json")
	if err == nil {
		log.Println("Read in test case")
	} else {
		log.Fatal(err)
	}
	//printUrls = true
	grabExpected := newGem()
	for _, tmp := range htc.GrabExpected {
		grabExpected.Add(tmp)
	}
	hm := NewHamster(promiscuous, allInteresting, printUrls, polite)
	dv := NewDomVisit("") // We don't want a bad filename
	for _, tmp := range htc.DvA {
		dv.VisitedA(tmp)
	}
	hm.SetDv(dv)
	fc := make(chan URL)
	gc := make(chan URL)
	hm.SetFetchCh(fc)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for url := range fc {
			log.Println("Received URL to fetch:", url)
		}
		wg.Done()
	}()
	go func() {
		for url := range gc {
			log.Println("Received URL to grab:", url)
			grabExpected.Visit(url.URL())
		}
		wg.Done()
	}()

	urlIn := NewURL(htc.URLToVisit)
	urlIn.SetShallow()
	hm.grabWo(urlIn, gc)
	close(fc)
	close(gc)
	wg.Wait()
	grabExpected.Close()
}

func TestHam1(t *testing.T) {
	if useTestParallel {
		t.Parallel()
	}
	var promiscuous, allInteresting, printUrls, polite bool
	htc, err := LoadHamsterTestCase("htc1.json")
	if err == nil {
		log.Println("Read in test case")
	} else {
		log.Fatal(err)
	}
	//printUrls = true
	grabExpected := newGem()
	for _, tmp := range htc.GrabExpected {
		grabExpected.Add(tmp)
	}
	fetchExpected := newGem()
	for _, tmp := range htc.FetchExpected {
		fetchExpected.Add(tmp)
	}

	hm := NewHamster(promiscuous, allInteresting, printUrls, polite)
	dv := NewDomVisit("") // We don't want a bad filename
	for _, tmp := range htc.DvA {
		dv.VisitedA(tmp)
	}
	hm.SetDv(dv)
	fc := make(chan URL)
	gc := make(chan URL)
	hm.SetFetchCh(fc)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for url := range fc {
			log.Println("Received URL to fetch:", url)
			fetchExpected.Visit(url.URL())
		}
		wg.Done()
	}()
	go func() {
		for url := range gc {
			log.Println("Received URL to grab:", url)
			grabExpected.Visit(url.URL())
		}
		wg.Done()
	}()

	urlIn := NewURL(htc.URLToVisit)
	urlIn.SetShallow()
	hm.grabWo(urlIn, gc)
	close(fc)
	close(gc)
	wg.Wait()
	grabExpected.Close()
	fetchExpected.Close()
}
