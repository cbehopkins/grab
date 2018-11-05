package grab

import (
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
)

func TestRunner0(t *testing.T) {
	if useTestParallel {
		t.Parallel()
	}
	rmFilename("visited.gkvlite")
	rmFilename("unvisit.gkvlite")
	var promiscuous, allInteresting, polite bool
	var multipleFetchers bool
	multipleFetchers = true
	//printUrls = true
	htc, err := LoadHamsterTestCase("rtc1.json")
	if err == nil {
		log.Println("Read in test case")
	} else {
		log.Fatal(err)
	}
	grabExpected := newGem()
	//for _, tmp := range htc.GrabExpected {
	//	grabExpected.Add(tmp)
	//}
	fetchExpected := newGem()
	for _, tmp := range htc.FetchExpected {
		fetchExpected.Add(tmp)
	}
	multiFetch := NewMultiFetch(multipleFetchers)
	multiFetch.SetDownload()
	fc := make(chan URL)
	gc := make(chan URL)
	runr := NewRunner(
		fc,
		"",    //badURLFn,
		".",   //*vdflg,
		false, // compact
		promiscuous,
		allInteresting,
		false, //debug
		polite,
	)
	//runr.SetLinear()
	multiFetch.Worker(runr)
	fmt.Println("Worker Started")

	//dv := NewDomVisit("") // We don't want a bad filename
	for _, tmp := range htc.DvA {
		runr.hm.dv.VisitedA(tmp)
	}
	//hm.SetDv(dv)
	//hm.SetFetchCh(fc)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for url := range fc {
			log.Println("Received URL to fetch:", url)
			fetchExpected.Visit(url.URL())
			multiFetch.InChan <- url
		}
		close(multiFetch.InChan)
		wg.Done()
	}()
	go func() {
		for url := range gc {
			log.Println("Received URL to grab:", url)
			grabExpected.Visit(url.URL())
		}
		wg.Done()
	}()

	seedChan := make(chan URL)
	wgrc := runr.RunChan(seedChan, "wgrcdbg")
	go func() {
		urlIn := NewURL(htc.URLToVisit)
		urlIn.SetShallow()
		seedChan <- urlIn
		close(seedChan)
	}()
	runr.ust.waitLoad()
	wgrc.Wait()
	fmt.Println("Seed Phase complete")
	runr.GrabRunner(1)
	fmt.Println("Unvisited Count:", runr.UnvisitCount())
	runr.Wait()
	log.Println("Shutting down fetcher")
	multiFetch.Shutdown()
	log.Println("Fetcher Shutdown")
	close(gc)
	wg.Wait()
	grabExpected.Close()
	fetchExpected.Close()
	runr.Shutdown()
	rmFilename("visited.gkvlite")
	rmFilename("unvisit.gkvlite")
	for _, opt := range htc.DvA {
		opt = GetBase(opt)
		fmt.Println("got opt", opt)
		if IsDir(opt) {
			fmt.Println("Found a directory to remove")
			err := os.RemoveAll(opt)
			if err != nil {
				log.Fatal("unable to remove directory:", err)
			}
		}
	}
}
