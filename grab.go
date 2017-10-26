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
func ProgressBars(r *grab.Runner, multi_fetch *grab.MultiFetch) *pb.Pool {
	// First keep track of max of things
	max_fc := multi_fetch.Count()

	// Create instances of the progress bars
	fet_bar := pb.New(multi_fetch.Count())
	url_bar := pb.New(r.VisitCount())

	// Name them
	fet_bar.Prefix("Images to Fetch:")
	url_bar.Prefix("Visited URLs :")
	fet_bar.Total = int64(max_fc)
	url_bar.Total = int64(r.UnvisitCount() + r.UnvisitCount())
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
			vc := r.VisitCount()
			url_bar.Total = int64(r.UnvisitCount() + vc)
			url_bar.Set(vc)
		}
	}()
	return pool
}

func shutdown(
	pool *pb.Pool, // The Progress Bars
	shutdown_in_progress *sync.Mutex,
	multi_fetch *grab.MultiFetch,
	runr *grab.Runner) {

	// Flag to get out debug
	debug_shutdown := true
	if debug_shutdown {
		fmt.Println("Shutdown process started")
	}

	if pool != nil {
		if debug_shutdown {
			fmt.Println("Stop Progress Bar")
		}
		pool.Stop()
	}
	if debug_shutdown {
		fmt.Println("Flush and Close")
	}

	if debug_shutdown {
		fmt.Println("Shutting down Grab Function")
	}
	runr.Shutdown()
	if debug_shutdown {
		fmt.Println("Shutdown the Fetch Function")
	}
	multi_fetch.Shutdown()

	// Wait for hamster to complete
	fmt.Println("Fetch Complete")

	shutdown_in_progress.Unlock()
	fmt.Println("Shutdown Complete")
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
  var linflg = flag.Bool("lin", false, "Linear Grab mode - sequentially progress through unvisited")
	var autopaceflg = flag.Int("apace", 0, "Automatically Pace the download")
	var rundurationflg = flag.Duration("dur", (2 * time.Hour), "Specify Run Duration")
	var dumpvisitedflg = flag.String("dumpv", "", "Write Visited URLs to file")
	var dumpunvisitflg = flag.String("dumpu", "", "Write Unvisited URLs to file")
	var vdflg = flag.String("vd", os.TempDir(), "Visited Directory")
	var num_p_fetch int
	flag.IntVar(&num_p_fetch, "numpar", 4, "Number of parallel fetches per domain")
	flag.Parse()
	show_progress_bar := !*dbgflg
	//show_progress_bar := false
	signalChan := make(chan os.Signal, 1)

	download := !*nodownflg
	multiple_fetchers := *mulflg && download
	promiscuous := *prflg
	if promiscuous {
		fmt.Printf("*\n*\n*\n*\n*\n*\nPromiscuous mode activated\n*\n*\n*\n*\n*\n*\n\n")
		//time.Sleep(5 * time.Second)
	}
	//shallow := !promiscuous
	all_interesting := *intrflg
	debug := *dbgflg

	url_fn := "in_urls.txt"
	fetch_fn := "gob_fetch.txt"
	bad_url_fn := "bad_urls.txt"

	// A fetch channel that goes away and writes intersting things to disk
	multi_fetch := grab.NewMultiFetch(multiple_fetchers)

	if download {
		multi_fetch.SetDownload()
	}
	if *testjpgflg {
		multi_fetch.SetTestJpg(2)
	}
	chan_fetch_push := multi_fetch.InChan

	runr := grab.NewRunner(chan_fetch_push,
		bad_url_fn, *vdflg,
		*compactflg,
		promiscuous,
		all_interesting,
		debug, // Print Urls
		*politeflg,
	)
  if *linflg {
  runr.SetLinear()
  }
	// We expect to be able to write to the fetch channel, so start the worker
	multi_fetch.Worker(runr)
	// If we have asked to dump the filenames to files
	runr.Dump(*dumpunvisitflg, *dumpvisitedflg)

	// After being read from the file they might first be crawled
	// alternatively this channel might not really exist at all
	// urls read from a file and command line

	if *clearvisitedflg {
		runr.ClearVisited()
		fmt.Println("Finsihed Resetting Unvisited")
	}
	fmt.Println("Seeding URLs")
	wgrc := runr.SeedWg(url_fn, promiscuous)
	wgrc.Wait()

	fmt.Println("Seed Phase complete")
	multi_fetch.SetFileName(fetch_fn)

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
		pool = ProgressBars(runr, multi_fetch)
		defer pool.Stop()
	}

	var shutdown_in_progress sync.Mutex
	var shutdown_run bool

	if rundurationflg != nil {
		go func() {
			// Run the main thing for no more than 100 Seconds/Minutes
			time.Sleep(*rundurationflg)
			fmt.Println("\n\nRuntime Exceeded\n\n")
			shutdown_in_progress.Lock()
			shutdown_run = true
			mem_profile(*memprofile)
			fmt.Println("Calling Shutdown")
			shutdown(pool, &shutdown_in_progress, multi_fetch, runr)
		}()
	}

	signal.Notify(signalChan, os.Interrupt)
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
					shutdown(pool, &shutdown_in_progress, multi_fetch, runr)
				}()
			} else {
				os.Exit(1)
			}
		}
	}()
	if *autopaceflg != 0 {
		go runr.AutoPace(multi_fetch, *autopaceflg)
	}
	//if *dbgflg {
	fmt.Println("Waiting for runner to complete")
	//}
	runr.Wait()
	if *dbgflg {
		fmt.Println("Runner complete. Waiting for fetch to complete")
	}
	multi_fetch.Close()
	multi_fetch.Wait()
	//if *dbgflg {
	fmt.Println("Fetch complete. Waiting for shutdown lock")
	//}

	shutdown_in_progress.Lock()
	//if *dbgflg {
	fmt.Println("Got shutdown lock. Adios!")
	//}
	if !shutdown_run {
		mem_profile(*memprofile)
		shutdown(pool, &shutdown_in_progress, multi_fetch, runr)
	} else {
		shutdown_in_progress.Unlock()
	}
}
