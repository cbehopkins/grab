package grab

import (
	"fmt"
	"image/jpeg"
	"io"
	"log"
	"net/http"
	"os"
	//"strings"
	"time"
)

// Test the jpeg for validity

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

const (
	OS_READ        = 04
	OS_WRITE       = 02
	OS_EX          = 01
	OS_USER_SHIFT  = 6
	OS_GROUP_SHIFT = 3
	OS_OTH_SHIFT   = 0

	OS_USER_R   = OS_READ << OS_USER_SHIFT
	OS_USER_W   = OS_WRITE << OS_USER_SHIFT
	OS_USER_X   = OS_EX << OS_USER_SHIFT
	OS_USER_RW  = OS_USER_R | OS_USER_W
	OS_USER_RWX = OS_USER_RW | OS_USER_X

	OS_GROUP_R   = OS_READ << OS_GROUP_SHIFT
	OS_GROUP_W   = OS_WRITE << OS_GROUP_SHIFT
	OS_GROUP_X   = OS_EX << OS_GROUP_SHIFT
	OS_GROUP_RW  = OS_GROUP_R | OS_GROUP_W
	OS_GROUP_RWX = OS_GROUP_RW | OS_GROUP_X

	OS_OTH_R   = OS_READ << OS_OTH_SHIFT
	OS_OTH_W   = OS_WRITE << OS_OTH_SHIFT
	OS_OTH_X   = OS_EX << OS_OTH_SHIFT
	OS_OTH_RW  = OS_OTH_R | OS_OTH_W
	OS_OTH_RWX = OS_OTH_RW | OS_OTH_X

	OS_ALL_R   = OS_USER_R | OS_GROUP_R | OS_OTH_R
	OS_ALL_W   = OS_USER_W | OS_GROUP_W | OS_OTH_W
	OS_ALL_X   = OS_USER_X | OS_GROUP_X | OS_OTH_X
	OS_ALL_RW  = OS_ALL_R | OS_ALL_W
	OS_ALL_RWX = OS_ALL_RW | OS_GROUP_X
)

func fetch_file(potential_file_name string, dir_str string, fetch_url Url) {
	// Create any directories needed to put this file in them
	var dir_file_mode os.FileMode
	dir_file_mode = os.ModeDir | (OS_USER_RWX | OS_ALL_R)
	os.MkdirAll(dir_str, dir_file_mode)

	out, err := os.Create(potential_file_name)
	switch t := err.(type) {
	case *os.PathError:
		switch t.Err {
		case os.ErrNotExist:
			log.Fatal("Invalid filename", potential_file_name)
		case os.ErrInvalid:
			log.Fatal("Invalid argument", potential_file_name)
		default:
			log.Fatalf("Que?\n\"%s\"\n%v\n,Dir:%s\nUrl:%s\n", potential_file_name, t.Err, dir_str, fetch_url)

		}
	case nil:
		// nothing
	default:
		log.Printf("Error is of type %T,n", err)
		check(err)

	}

	defer out.Close()

	if fetch_url.Url() == "" {
		//fmt.Println("null fetch")
		return
	}
	timeout := time.Duration(10 * time.Minute)
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Get(fetch_url.Url())
	// TBD Add error handling here
	if err != nil {
		return
	}
	_ = DecodeHttpError(err)
	defer resp.Body.Close()
	_, _ = io.Copy(out, resp.Body)
	//check(err)
}
func check_jpg(filename string) bool {
	out, err := os.Open(filename)
	check(err)
	defer out.Close()
	_, err = jpeg.Decode(out)
	if err == nil {
	}
	switch err.(type) {
	case nil:
		return true
	case jpeg.FormatError:
		switch err.Error() {
		case "invalid JPEG format: short Huffman data":
			return false
		case "invalid JPEG format: missing SOI marker":
			return false
		default:
			fmt.Printf("Unknown jpeg Error Text type:%T, Value %v\n", err, err)
			panic(err)
		}
	default:
		switch err.Error() {
		case "unexpected EOF":
			return false
		case "EOF":
			return false
		default:
			fmt.Printf("Unknown jpeg Error type:%T, Value %v\n", err, err)
			panic(err)
		}
	}
}

//type Fetcher struct {
//	// Log file of files we have fetched
//	file         *os.File
//	run_download bool
//	factive      bool
//	// Files to fetch come in on this channel
//	chan_fetch_pop UrlChannel
//	// Effectively a Waitgroup to allow us to know when we are busy
//	out_count *OutCounter
//	// Our way of managing how many fetches we have running at once
//	fetch_token_chan *TokenChan
//	// If the file already exists should we load it in to check if it is valid?
//	test_jpg bool
//	dbg_urls bool
//}
//
//func NewFetcher(chan_fetch_pop UrlChannel, out_count *OutCounter, fetch_token_chan *TokenChan) *Fetcher {
//	itm := new(Fetcher)
//	itm.run_download = true
//	itm.chan_fetch_pop = chan_fetch_pop
//	itm.out_count = out_count
//	itm.fetch_token_chan = fetch_token_chan
//
//	return itm
//}
//func (f Fetcher) Start() {
//	go f.FetchReceiver()
//
//}
//func (f *Fetcher) SetTestJpg(vary bool) {
//	f.test_jpg = vary
//}
//func (f *Fetcher) SetRunDownload(vary bool) {
//	f.run_download = vary
//}
//func (f Fetcher) Fetch(fetch_url Url) bool {
//
//	n_url := strings.Replace(fetch_url.Url(), "\n", "", -1)
//	fetch_url = NewUrl(n_url)
//	if strings.HasSuffix(fetch_url.Url(), "/") {
//		fetch_url_htm := NewUrl(fetch_url.Url() + "index.htm")
//		fmt.Printf("Trying to fetch %s with htm extension\n", fetch_url_htm)
//		if FetchW(fetch_url_htm, f.test_jpg) {
//			return true
//		} else {
//			// Try again with the .html extension
//			fetch_url_htm = NewUrl(fetch_url_htm.Url() + "l")
//			fmt.Printf("That clearly failed, Trying to fetch %s with html extension\n", fetch_url_htm)
//			return FetchW(fetch_url_htm, f.test_jpg)
//		}
//	}
//	return FetchW(fetch_url, f.test_jpg)
//}
//
//func (f *Fetcher) DbgFile(fetch_file string) {
//	if fetch_file != "" {
//		var err error
//		f.file, err = os.Create(fetch_file)
//		if err != nil {
//			log.Fatal("Cannot create file", err)
//		}
//
//		f.factive = true
//	}
//}
//func (f *Fetcher) SetDbgUrls(vary bool) {
//	f.dbg_urls = vary
//}
//func (f Fetcher) FetchReceiver() {
//
//	//fetch_sim_chan := *NewTokenChan(0, 8, "")
//	if f.factive {
//		defer f.file.Close()
//	}
//
//	fetched_urls := make(map[string]bool)
//	for fetch_url := range f.chan_fetch_pop {
//
//		_, ok := fetched_urls[fetch_url.Url()]
//		if !ok {
//			fetched_urls[fetch_url.Url()] = true
//
//			// Two tokens are needed to proceed
//			// The first is to make sure in no circumstances do we have nore than N trying to process stuff
//			// i.e. that there are not too many things happening at once
//			//fetch_sim_chan.GetToken(getBase(fetch_url.Url()))
//
//			// The second token is rate limiting making sure we don't make a request too frequently
//			f.fetch_token_chan.GetToken(fetch_url.Base())
//			go func(f_url Url) {
//				// When we're done then always return the simultaneous limit token
//				//defer fetch_sim_chan.PutToken(getBase(string(f_url)))
//				var used_network bool
//				if f.run_download {
//					if f.dbg_urls {
//						//	fmt.Printf("FetchReceiver Fetching %s\n", f_url)
//					}
//					used_network = f.Fetch(f_url)
//				}
//				if f.factive {
//					fmt.Fprintf(f.file, "%s\n", f_url.Url())
//				}
//				// If we have not used the network then return the Rate lmit token
//				if !used_network {
//					//fmt.Println("Not used fetch token, returning")
//					// Tries to put the token if there is space available
//					// otherwise doesn't do anything - avoids locking
//					f.fetch_token_chan.TryPutToken(f_url.Base())
//				} else {
//					// PutToken puts it back if in the right mode
//					f.fetch_token_chan.PutToken(f_url.Base())
//				}
//				f.out_count.Dec()
//			}(fetch_url)
//		} else {
//			f.out_count.Dec()
//		}
//	}
//}
