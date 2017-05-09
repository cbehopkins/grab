package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/cbehopkins/grab/grab"
	"github.com/cheggaaa/pb"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}
func ProgressBars(visited_urls *grab.UrlMap, unvisit_urls *grab.UrlMap, multi_fetch *grab.MultiFetch) *pb.Pool {
	// First keep track of max of things
	max_fc := multi_fetch.Count()

	// Create instances of the progress bars
	fet_bar := pb.New(multi_fetch.Count())
	url_bar := pb.New(visited_urls.Count())

	// Name them
	fet_bar.Prefix("Images to Fetch:")
	url_bar.Prefix("Visited URLs :")
	fet_bar.Total = int64(max_fc)
	url_bar.Total = int64(unvisit_urls.Count() + visited_urls.Count())
	// and start them
	pool, err := pb.StartPool(fet_bar, url_bar)
	if err != nil {
		panic(err)
	}
	// Start a routine that will occasionally update them
	go func() {
		fet_bar.Total = int64(multi_fetch.TotCnt())
		for {
			time.Sleep(10 * time.Second)

			// TBD If equal then zero both
			if multi_fetch.TotCnt() > max_fc {
				max_fc = multi_fetch.TotCnt()
				fet_bar.Total = int64(max_fc)
			}
			fet_bar.Set(max_fc - multi_fetch.Count())
			vc := visited_urls.Count()
			url_bar.Total = int64(unvisit_urls.Count() + vc)
			url_bar.Set(vc)
		}
	}()
	return pool
}

func shutdown(
	pool *pb.Pool, // The Progress Bars
	unvisit_urls, visited_urls *grab.UrlMap,
	shutdown_in_progress *sync.Mutex,
	multi_fetch *grab.MultiFetch,
	runr *grab.Runner) {
	runr.Resume()
	// First stop the Progress bar
	//fmt.Println("Stop Pool\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
	if pool != nil {
		pool.Stop()
	}
	//fmt.Println("flush and close\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
	var closing_wg sync.WaitGroup
	closing_wg.Add(1)
	go func() {
		// Performance tweek - get the activity to disk started asap
		unvisit_urls.Flush()
		visited_urls.Flush()
		unvisit_urls.Sync()
		visited_urls.Sync()
		closing_wg.Done()
	}()
	// Shutdown the Grab function
	runr.Shutdown()
	// Shutdown the Fetch Function
	multi_fetch.Shutdown()

	// Wait for hamster to complete
	fmt.Println("Fetch Complete")
	closing_wg.Wait()
	unvisit_urls.Close()
	visited_urls.Close()
	shutdown_in_progress.Unlock()
	fmt.Println("All Complete")
}
func mem_profile(mem_prf_fn string) {
	if mem_prf_fn != "" {
		f, err := os.Create(mem_prf_fn)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}

func main() {
	var memprofile = flag.String("memprofile", "", "write memory profile to this file")
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var mulflg = flag.Bool("multi", true, "Use multiple Fetchers")
	var prflg = flag.Bool("prom", false, "Promiscuously Fetch everything allowed")
	var intrflg = flag.Bool("interest", false, "All Links found are Interesting - Super promiscuous mode")
	var dbgflg = flag.Bool("dbg", false, "Debug Mode")
	var nodownflg = flag.Bool("nod", false, "No Download Mode")
	var compactflg = flag.Bool("compact", false, "Compact Databases")
	var politeflg = flag.Bool("polite", false, "Respect robots.txt")
	var gofastflg = flag.Bool("fast", false, "Go Fast")
	var clearvisitedflg = flag.Bool("clearv", false, "Clear All visited into Unvisited")
	var testjpgflg = flag.Bool("tjpg", true, "Test Jpgs for validity")
	var autopaceflg = flag.Int("apace", 0, "Automatically Pace the download")
	var rundurationflg = flag.Duration("dur", (2 * time.Hour), "Specify Run Duration")
	var dumpvisitedflg = flag.String("dumpv", "", "Write Visited URLs to file")
	var dumpunvisitflg = flag.String("dumpu", "", "Write Unvisited URLs to file")
	var num_p_fetch int
	flag.IntVar(&num_p_fetch, "numpar", 4, "Number of parallel fetches per domain")
	flag.Parse()
	show_progress_bar := !*dbgflg
	//show_progress_bar := false

	var wg sync.WaitGroup
	download := !*nodownflg
	multiple_fetchers := *mulflg && download
	promiscuous := *prflg
	if promiscuous {
		fmt.Printf("*\n*\n*\n*\n*\n*\nPromiscuous mode activated\n*\n*\n*\n*\n*\n*\n\n")
		time.Sleep(5 * time.Second)
	}
	shallow := !promiscuous
	all_interesting := *intrflg
	debug := *dbgflg

	print_workload := false

	visited_fname := "/tmp/visited.gkvlite"
	unvisit_fname := "/tmp/unvisit.gkvlite"
	url_fn := "in_urls.txt"
	fetch_fn := "gob_fetch.txt"
	bad_url_fn := "bad_urls.txt"

	// A fetch channel that goes away and writes intersting things to disk
	multi_fetch := grab.NewMultiFetch(multiple_fetchers)
	multi_fetch.SetFileName(fetch_fn)

	if download {
		multi_fetch.SetDownload()
	}
	if *testjpgflg {
		multi_fetch.SetTestJpg(2)
	}
	chan_fetch_push := multi_fetch.InChan

	var visited_urls *grab.UrlMap
	var unvisit_urls *grab.UrlMap
	// Open the maps and do not overwrite any we find
	visited_urls = grab.NewUrlMap(visited_fname, false, *compactflg)
	unvisit_urls = grab.NewUrlMap(unvisit_fname, false, *compactflg)
	visited_urls.SetWriteCache()
	visited_urls.SetReadCache()
	unvisit_urls.SetWriteCache()
	unvisit_urls.SetReadCache()
	if *clearvisitedflg {
		list := make([]grab.Url, 0, 10000)
		s := grab.Spinner{}
		cnt := 0
		length := visited_urls.Count()
		fmt.Println("Resetting Unvisited", length)
		for visited_urls.Size() > 0 {
			cnt_backup := cnt
			visited_chan := visited_urls.Visit()
			for v := range visited_chan {
				list = append(list, v)
				s.PrintSpin(cnt)
				cnt++
			}
			cnt = cnt_backup
			for _, v := range list {
				visited_urls.Delete(v)
				unvisit_urls.Set(v)
				s.PrintSpin(cnt)
				cnt++
			}

			// reset list to 0 length - but retain capacity
			list = list[:0]
		}
		fmt.Println("Finsihed Resetting Unvisited")
	}

	if *dumpvisitedflg != "" {
		the_chan := visited_urls.VisitAll()
		grab.SaveFile(*dumpvisitedflg, the_chan, nil)
	}
	if *dumpunvisitflg != "" {
		the_chan := unvisit_urls.VisitAll()
		grab.SaveFile(*dumpunvisitflg, the_chan, nil)
	}

	// A DomVisit tracks what domains we're allowed to visit
	// Any domains we come across not in this list will not be visited
	dmv := grab.NewDomVisit()
	defer dmv.Close()
	multi_fetch.Worker(dmv)

	// The Hamster is the thing that goes out and
	// Grabs stuff, looking for interesting things to fetch
	hm := grab.NewHamster(
		promiscuous,
		shallow,
		all_interesting,
		debug, // Print Urls
	)
	if *politeflg {
		hm.Polite()
	}
	hm.SetDv(dmv)
	hm.SetFetchCh(chan_fetch_push)

	fmt.Println("Seeding URLs")
	wg.Add(1) // one for badUrls
	go func() {
		bad_url_chan := *grab.NewUrlChannel()
		go grab.LoadFile(bad_url_fn, bad_url_chan, nil, true, false)
		for itm := range bad_url_chan {
			dmv.AddBad(itm.Url())
		}
		wg.Done()
	}()
	// After being read from the file they might first be crawled
	// alternatively this channel might not really exist at all
	var start_url_chan chan grab.Url

	// urls read from a file and command line
	src_url_chan := make(chan grab.Url)
	wgrc := grab.RunChan(src_url_chan, visited_urls, unvisit_urls, "")

	go func() {
		if shallow {
			// If in shallow mode then first write to shallow chan
			// then have a process that Grabs those URLs
			start_url_chan = make(chan grab.Url)
		} else {
			// If not in shallow mode, write direct to
			// the main source channel
			// which will sort them into the *visit* maps
			start_url_chan = src_url_chan
		}

		seed_url_chan := *grab.NewUrlChannel()
		go grab.LoadFile(url_fn, seed_url_chan, nil, true, false)
		for itm := range seed_url_chan {
			//fmt.Println("SeedURL:", itm)

			domain_i := itm.Base()
			if domain_i != "" {
				// Mark this as a domain we can Fetch from
				_ = dmv.VisitedA(domain_i)
				// send this URL for grabbing
				start_url_chan <- itm
			}
		}
		fmt.Println("seed_url_chan seen closed")
		close(start_url_chan)
	}()

	if shallow {
		wg.Add(1) // one for shallow
		go func() {
			// Now don't get confused by how a shallow hamster behaves
			// With shallow set, we actually get the same behaviour as promiscuous being set
			// The point is we do a single pass looking for things to add to grab
			// Later we clear shallow bit
			// The URLS are still then grabbed and processed for interesting things
			// but without promiscuous or shallow set, then we never look for
			// new Urls within the page that might be suitable for further grabbing
			hms := grab.NewHamster(
				promiscuous,
				shallow,
				all_interesting,
				debug, // Print Urls
			)
			hms.SetDv(dmv)
			hms.SetFetchCh(chan_fetch_push)
			var grab_tk_rep *grab.TokenChan
			grab_tk_rep = grab.NewTokenChan(num_p_fetch, "shallow")
			hms.SetGrabCh(src_url_chan)
			for itm := range start_url_chan {
				//fmt.Println("Shallow Grab:", itm)
				hms.GrabT(
					itm, // The URL we are tasked with crawling
					"",
					grab_tk_rep,
				)
				//fmt.Println("Grabbed:",itm)
			}
			// We close it now because after a shallow crawl
			// there should be no new URLs added to be crawled
			close(src_url_chan)
			wg.Done()
		}()
	}
	wg.Wait()
	wgrc.Wait()
	fmt.Println("Run Seeded")
	hm.ClearShallow()

	if print_workload {
		fmt.Println("Printing Workload")
		unvisit_urls.PrintWorkload()

		go func() {
			time.Sleep(10 * time.Minute)
			unvisit_urls.PrintWorkload()
			fmt.Println("")
			fmt.Println("")
		}()
		fmt.Println("Workload Printed")
	}
	// Now we're up and running
	// Start the profiler
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer f.Close()
		defer pprof.StopCPUProfile()
	}

	runr := grab.NewRunner(hm, unvisit_urls, visited_urls)

	if !*gofastflg {
		runr.GoSlow()
		fmt.Println("Grab Slowly")
	} else {
		fmt.Println("Rapid Grab")
	}
	runr.GrabRunner(
		num_p_fetch,
	)

	var pool *pb.Pool
	if show_progress_bar {
		// Show a progress bar
		pool = ProgressBars(visited_urls, unvisit_urls, multi_fetch)
		defer pool.Stop()
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	var shutdown_in_progress sync.Mutex
	var shutdown_run bool

	if rundurationflg != nil {
		go func() {
			// Run the main thing for no more than 100 Seconds/Minutes
			time.Sleep(*rundurationflg)
			shutdown_in_progress.Lock()
			shutdown_run = true
			fmt.Println("Runtime Exceeded")
			mem_profile(*memprofile)
			shutdown(pool, unvisit_urls, visited_urls, &shutdown_in_progress, multi_fetch, runr)
		}()
	}

	go func() {
		cc_cnt := 0
		for _ = range signalChan {
			cc_cnt++
			if cc_cnt == 1 {
				shutdown_in_progress.Lock()
				go func() {
					fmt.Println("Ctrl-C Detected")
					shutdown_run = true
					mem_profile(*memprofile)
					shutdown(pool, unvisit_urls, visited_urls, &shutdown_in_progress, multi_fetch, runr)
				}()
			} else {
				os.Exit(1)
			}
		}
	}()
	if *autopaceflg != 0 {
		go runr.AutoPace(multi_fetch, *autopaceflg)
	}

	runr.Wait()
	multi_fetch.Wait()
	shutdown_in_progress.Lock()
	if !shutdown_run {
		mem_profile(*memprofile)
		shutdown(pool, unvisit_urls, visited_urls, &shutdown_in_progress, multi_fetch, runr)
	} else {
		shutdown_in_progress.Unlock()
	}
}
