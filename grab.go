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
func progressBars(r *grab.Runner, multiFetch *grab.MultiFetch) *pb.Pool {
	// First keep track of max of things
	maxFc := multiFetch.Count()

	// Create instances of the progress bars
	fetBar := pb.New(multiFetch.Count())
	urlBar := pb.New(r.VisitCount())

	// Name them
	fetBar.Prefix("Images to Fetch:")
	urlBar.Prefix("Visited URLs :")
	fetBar.Total = int64(maxFc)
	urlBar.Total = int64(r.UnvisitCount() + r.UnvisitCount())
	// and start them
	pool, err := pb.StartPool(fetBar, urlBar)
	if err != nil {
		panic(err)
	}
	// Start a routine that will occasionally update them
	go func() {
		fetBar.Total = int64(multiFetch.TotCnt())
		for {
			time.Sleep(10 * time.Second)

			// TBD If equal then zero both
			if multiFetch.TotCnt() > maxFc {
				maxFc = multiFetch.TotCnt()
				fetBar.Total = int64(maxFc)
			}
			fetBar.Set(maxFc - multiFetch.Count())
			vc := r.VisitCount()
			urlBar.Total = int64(r.UnvisitCount() + vc)
			urlBar.Set(vc)
		}
	}()
	return pool
}

func shutdown(
	pool *pb.Pool, // The Progress Bars
	shutdownInProgress *sync.Mutex,
	multiFetch *grab.MultiFetch,
	runr *grab.Runner) {

	// Flag to get out debug
	debugShutdown := true
	if debugShutdown {
		fmt.Println("Shutdown process started")
	}

	if pool != nil {
		if debugShutdown {
			fmt.Println("Stop Progress Bar")
		}
		pool.Stop()
	}
	if debugShutdown {
		fmt.Println("Flush and Close")
	}

	if debugShutdown {
		fmt.Println("Shutting down grab Function")
	}
	runr.Shutdown()
	if debugShutdown {
		fmt.Println("Shutdown the Fetch Function")
	}
	multiFetch.Shutdown()

	// Wait for hamster to complete
	fmt.Println("Fetch Complete")

	shutdownInProgress.Unlock()
	fmt.Println("Shutdown Complete")
}
func memProfile(memPrfFn string) {
	if memPrfFn != "" {
		f, err := os.Create(memPrfFn)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}
func isDir(path string) bool {
	if stat, err := os.Stat(path); err == nil && stat.IsDir() {
		return true
	}
	return false
}
func main() {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal("Unable to get current directory", err)
	}
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
	var linflg = flag.Bool("lin", true, "Linear grab mode - sequentially progress through unvisited")
	var autopaceflg = flag.Int("apace", 0, "Automatically Pace the download")
	var rundurationflg = flag.Duration("dur", 2*time.Hour, "Specify Run Duration")
	var dumpvisitedflg = flag.String("dumpv", "", "Write Visited URLs to file")
	var dumpunvisitflg = flag.String("dumpu", "", "Write Unvisited URLs to file")
	var numPFetch int
	flag.IntVar(&numPFetch, "numpar", 4, "Number of parallel fetches per domain")
	var wdflag = flag.String("wd", cwd, "Set Working Directory")
	var vdflg = flag.String("vd", os.TempDir(), "Visited Directory")
	var vwdflag = flag.String("vwd", "", "Set Working and Visited directory")
	flag.Parse()
	if *vwdflag == "" {
		// Default value - nothing to do
	} else {
		if isDir(*vwdflag) {
			wdflag = vwdflag
			vdflg = vwdflag
		}
	}
	err = os.Chdir(*wdflag)
	if err != nil {
		log.Fatal("Unable to change to working directory", *wdflag, err)
	}
	showProgressBar := !*dbgflg
	debug := *dbgflg
	//showProgressBar := true
  //debug := false
	signalChan := make(chan os.Signal, 1)

	download := !*nodownflg
	multipleFetchers := *mulflg && download
	promiscuous := *prflg
	if promiscuous {
		fmt.Printf("*\n*\n*\n*\n*\n*\nPromiscuous mode activated\n*\n*\n*\n*\n*\n*\n\n")
		//time.Sleep(5 * time.Second)
	}
	//shallow := !promiscuous
	allInteresting := *intrflg

	urlFn := "in_urls.txt"
	fetchFn := "gob_fetch.txt"
	badURLFn := "bad_urls.txt"
	fmt.Println("Setting up mf")
	// A fetch channel that goes away and writes intersting things to disk
	multiFetch := grab.NewMultiFetch(multipleFetchers)
	if debug {
		multiFetch.SetDebug()
	}

	if download {
		multiFetch.SetDownload()
	}
	if *testjpgflg {
		multiFetch.SetTestJpg(2)
	}
	chanFetchPush := multiFetch.InChan
	fmt.Println("Setting up runner")
	runr := grab.NewRunner(chanFetchPush,
		badURLFn, *vdflg,
		*compactflg,
		promiscuous,
		allInteresting,
		debug, // Print Urls
		*politeflg,
	)
	if *linflg {
		fmt.Println("Running In Linear Mode")
		runr.SetLinear()
	}
	// We expect to be able to write to the fetch channel, so start the worker
	multiFetch.Worker(runr)
	fmt.Println("Worker Started")
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
	wgrc := runr.SeedWg(urlFn, promiscuous)
	wgrc.Wait()

	fmt.Println("Seed Phase complete")
	// Read in the Gob
	multiFetch.SetFileName(fetchFn)

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
		fmt.Println("grab Slowly")
	} else {
		fmt.Println("Rapid grab")
	}
	if *autopaceflg != 0 {
		runr.Pause()
		go runr.AutoPace(multiFetch, *autopaceflg)
	}
	runr.GrabRunner(
		numPFetch,
	)

	var pool *pb.Pool
	if showProgressBar {
		logFileName := "grab_new.log"
		logf, err := os.OpenFile(logFileName, os.O_WRONLY|os.O_CREATE, 0640)
		if err != nil {
			log.Fatalln(err)
		}
		log.SetOutput(logf)
    fmt.Println("Log sent to file")

		// Show a progress bar
		pool = progressBars(runr, multiFetch)
		defer pool.Stop()
	}

	var shutdownInProgress sync.Mutex
	var shutdownRun bool

	if rundurationflg != nil {
		go func() {
			// Run the main thing for no more than 100 Seconds/Minutes
			time.Sleep(*rundurationflg)
			fmt.Printf("\n\nRuntime Exceeded\n\n")
			shutdownInProgress.Lock()
			shutdownRun = true
			memProfile(*memprofile)
			fmt.Println("Calling Shutdown")
			shutdown(pool, &shutdownInProgress, multiFetch, runr)
		}()
	}

	signal.Notify(signalChan, os.Interrupt)
	go func() {
		ccCnt := 0
		for range signalChan {
			ccCnt++
			if ccCnt == 1 {
				shutdownInProgress.Lock()
				go func() {
					fmt.Println("Ctrl-C Detected")
					shutdownRun = true
					memProfile(*memprofile)
					shutdown(pool, &shutdownInProgress, multiFetch, runr)
				}()
			} else {
				os.Exit(1)
			}
		}
	}()
	//if *dbgflg {
	fmt.Println("Waiting for runner to complete")
	//}
	runr.Wait()
	if *dbgflg {
		fmt.Println("Runner complete. Waiting for fetch to complete")
	}
	multiFetch.Close()
	multiFetch.Wait()
	//if *dbgflg {
	fmt.Println("Fetch complete. Waiting for shutdown lock")
	//}

	shutdownInProgress.Lock()
	//if *dbgflg {
	fmt.Println("Got shutdown lock. Adios!")
	//}
	if !shutdownRun {
		memProfile(*memprofile)
		shutdown(pool, &shutdownInProgress, multiFetch, runr)
	} else {
		shutdownInProgress.Unlock()
	}
}
