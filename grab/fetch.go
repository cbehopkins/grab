package grab

import (
	"fmt"
	"image/jpeg"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

// Test the jpeg for validity
var test_jpg bool

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
func fetch_file(potential_file_name string, dir_str string, fetch_url Url) {
	os.MkdirAll(dir_str, os.ModeDir)

	out, err := os.Create(potential_file_name)
	check(err)
	defer out.Close()
	if fetch_url == "" {
		fmt.Println("null fetch")
	}

	resp, err := http.Get(string(fetch_url))
	// TBD Add error handling here
	_ = DecodeHttpError(err)
	defer resp.Body.Close()
	_, err = io.Copy(out, resp.Body)
	check(err)
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
		default:
			fmt.Printf("Unknown jpeg Error Text type:%T, Value %v\n", err, err)
			panic(err)
		}
	default:
		switch err.Error() {
		case "EOF":
			return false
		default:
			fmt.Printf("Unknown jpeg Error type:%T, Value %v\n", err, err)
			panic(err)
		}
	}
}

type Fetcher struct {
	file             *os.File
	run_download     bool
	factive          bool
	chan_fetch_pop   UrlChannel
	out_count        *OutCounter
	fetch_token_chan TokenChan
	num_sim          int
}

func NewFetcher(chan_fetch_pop UrlChannel, out_count *OutCounter, fetch_token_chan TokenChan, num_sim int) *Fetcher {
	itm := new(Fetcher)
	itm.run_download = true
	go itm.FetchReceiver()

	return itm
}
func (f Fetcher) Fetch(fetch_url Url) bool {
	array := strings.Split(string(fetch_url), "/")
	var fn string
	if len(array) > 0 {
		fn = array[len(array)-1]
	}

	dir_struct := array[2 : len(array)-1]
	dir_str := strings.Join(dir_struct, "/")
	potential_file_name := dir_str + "/" + fn
	if _, err := os.Stat(potential_file_name); os.IsNotExist(err) {
		//fmt.Printf("Fetching %s, fn:%s\n", fetch_url, fn)
		fetch_file(potential_file_name, dir_str, fetch_url)
		return true
	} else {
		if !test_jpg {
			return false
		}
		//fmt.Println("skipping downloading", potential_file_name)
		good_file := check_jpg(potential_file_name)
		if good_file {
			return false
		} else {
			//fmt.Printf("Fetching %s, fn:%s\n", fetch_url, fn)
			fetch_file(potential_file_name, dir_str, fetch_url)
			return true
		}
	}
}
func (f *Fetcher) DbgFile(fetch_file string) {
	if fetch_file != "" {
		var err error
		f.file, err = os.Create(fetch_file)
		if err != nil {
			log.Fatal("Cannot create file", err)
		}

		f.factive = true
	}
}
func (f Fetcher) FetchReceiver() {

	fetch_sim_chan := *NewTokenChan(0, 8, "")
	if f.factive {
		defer f.file.Close()
	}

	fetched_urls := make(map[Url]bool)
	for fetch_url := range f.chan_fetch_pop {
		_, ok := fetched_urls[fetch_url]
		if !ok {
			fetched_urls[fetch_url] = true
			// Two tokens are needed to proceed
			// The first is to make sure in no circumstances do we have nore than N trying to process stuff
			// i.e. that there are not too many things happening at once
			fetch_sim_chan.GetToken()

			// The second token is rate limiting making sure we don't make a request too frequently
			f.fetch_token_chan.GetToken()
			go func() {
				defer fetch_sim_chan.PutToken()
				var used_network bool
				if f.run_download {
					used_network = f.Fetch(fetch_url)
				} else {
					// Pretend we used it to make sure there is a sink of tokens - otherwise we can lock up
					used_network = true
				}
				//fmt.Println("Fetching :", fetch_url)
				if f.factive {
					fmt.Fprintf(f.file, "%s\n", string(fetch_url))
				}
				if !used_network {
					//fmt.Println("Not used fetch token, returning")
					//fetch_token_chan.PutToken()
				}
				f.out_count.Dec()
			}()
		} else {
			f.out_count.Dec()
		}
	}
}
