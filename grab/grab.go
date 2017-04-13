package grab

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
  "sync"

	"golang.org/x/net/html"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// Helper function to pull the href attribute from a Token
func getHref(t html.Token) (ok bool, href string) {
	// Iterate over all of the Token's attributes until we find an "href"
	for _, a := range t.Attr {
		if a.Key == "href" {
			href = a.Val
			ok = true
		}
	}

	// "bare" return will return the variables (ok, href) as defined in
	// the function definition
	return
}
func Readln(r *bufio.Reader) (string, error) {
	var (
		isPrefix bool  = true
		err      error = nil
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}

func LoadFile(filename string, the_chan chan Url, counter *OutCounter, close_chan, inc_count bool) {
	if counter != nil {
		defer counter.Dec()
	}
	if close_chan {
		defer close(the_chan)
	}
	if filename == "" {
		return
	}
	f, err := os.Open(filename)
	if err == os.ErrNotExist {
		return
	} else if err != nil {
		if os.IsNotExist(err) {
			return
		} else {
			fmt.Printf("error opening file: %T\n", err)
			os.Exit(1)
			return
		}
	}
	defer f.Close()
	r := bufio.NewReader(f)
	s, e := Readln(r)
	for e == nil {
		//fmt.Println("Fast adding ", s)
		if inc_count {
			counter.Add()
		}
		the_chan <- NewUrl(s)
		s, e = Readln(r)
	}
}
func SaveFile(filename string, the_chan chan Url, counter *OutCounter) {
	if counter != nil {
		defer counter.Dec()
	}
	if filename == "" {
		return
	}
	f, err := os.Create(filename)
	check(err)
	defer f.Close()
	for v := range the_chan {
		fmt.Fprintf(f, "%s\n", v.Url())
	}
}
func LoadGob(filename string, the_chan chan Url, counter *OutCounter, close_chan, inc_count bool) {
	if counter != nil {
		defer counter.Dec()
	}
	if close_chan {
		defer close(the_chan)
	}
	if filename == "" {
		return
	}
	f, err := os.Open(filename)
	if err == os.ErrNotExist {
		return
	} else if err != nil {
		if os.IsNotExist(err) {
			return
		} else {
			fmt.Printf("error opening file: %T\n", err)
			os.Exit(1)
			return
		}
	}
	defer os.Remove(filename)
	defer f.Close()
	fi, err := f.Stat()
	check(err)
	if fi.Size() > 1 {
		//fmt.Printf("Opened filename:%s,%d\n", filename, fi.Size())
		buff := make([]Url, 0)
		dec := gob.NewDecoder(f)
		err = dec.Decode(&buff)
		check(err)
		for _, v := range buff {
			the_chan <- v
		}

	}
}
func SaveGob(filename string, the_chan chan Url, counter *OutCounter) {
	if counter != nil {
		defer counter.Dec()
	}
	if filename == "" {
		return
	}
	f, err := os.Create(filename)
	check(err)
	defer f.Close()
	buff := make([]Url, 0)
	fmt.Println("Start bufferring")
	for v := range the_chan {
		buff = append(buff, v)
	}
	fmt.Println("Finished buffering")
	enc := gob.NewEncoder(f)
	err = enc.Encode(buff)
	fmt.Println("Finished gobbing")
	check(err)
}
func GetBase(urls string) string {
	var ai *url.URL
	var err error
	ai, err = url.Parse(urls)
	check(err)
	hn := ai.Hostname()
	if hn == "http" || hn == "nats" || strings.Contains(hn, "+document.location.host+") {
		return ""
	} else {
		return hn
	}
}
func RunChan(input_chan <-chan Url, visited_urls, unvisit_urls *UrlMap, dbg_name string) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for urv := range input_chan {
			if dbg_name != "" {
				fmt.Printf("runChan, %s RX:%v\n", dbg_name, urv)
			}
			if visited_urls.Exist(urv) {
				// If we've already visited it then nothing to do
				//fmt.Printf("We've already visited %s\n", urv)
			} else {
				// If we haven't visited it, then add it to the list of places to visit
				unvisit_urls.Set(urv)
			}
			if dbg_name != "" {
				fmt.Printf("runChan, %s, has been set\n", dbg_name)
			}
		}
		wg.Done()
	}()
	return &wg
}

func FetchW(fetch_url Url, test_jpg bool) bool {
	// We retun true if we have used network bandwidth.
	// If we have not then it's okay to jump straight onto the next file
	array := strings.Split(fetch_url.Url(), "/")

	var fn string
	if len(array) > 2 {
		fn = array[len(array)-1]
	} else {
		return false
	}
	if strings.HasPrefix(fetch_url.Url(), "file") {
		return false
	}

	fn = strings.TrimLeft(fn, ".php?")
	// logically there must be http:// so therefore length>2
	dir_struct := array[2 : len(array)-1]
	dir_str := strings.Join(dir_struct, "/")
	dir_str = strings.Replace(dir_str, "//", "/", -1)
	dir_str = strings.Replace(dir_str, "%", "_", -1)
	dir_str = strings.Replace(dir_str, "&", "_", -1)
	dir_str = strings.Replace(dir_str, "?", "_", -1)
	dir_str = strings.Replace(dir_str, "=", "_", -1)

	re := regexp.MustCompile("(.*\\.jpg)(.*)")
	t1 := re.FindStringSubmatch(fn)
	if len(t1) > 1 {
		fn = t1[1]
	}
	page_title := fetch_url.GetTitle()
	if page_title != "" {
		page_title = strings.Replace(page_title, "/", "_", -1)
		if strings.HasSuffix(fn, ".mp4") {
			fn = page_title + ".mp4"
			//fmt.Println("Title set, so set filename to:", fn)
		}
	}
	fn = strings.Replace(fn, "%", "_", -1)
	fn = strings.Replace(fn, "&", "_", -1)
	fn = strings.Replace(fn, "?", "_", -1)
	fn = strings.Replace(fn, "=", "_", -1)
	fn = strings.Replace(fn, "\"", "_", -1)
	fn = strings.Replace(fn, "'", "_", -1)
	fn = strings.Replace(fn, "!", "_", -1)
	fn = strings.Replace(fn, ",", "_", -1)
	potential_file_name := dir_str + "/" + fn
	if strings.HasPrefix(potential_file_name, "/") {
		return false
	}
	if _, err := os.Stat(potential_file_name); !os.IsNotExist(err) {
		// For a file that does already exist
		if !test_jpg {
			// We're not testing all the jpgs for goodness
			//fmt.Println("skipping downloading", potential_file_name)
			return false
		}
		// Check if it is a corrupted file. If it is, then fetch again
		good_file := check_jpg(potential_file_name)
		if good_file {
			return false
		}
	}

	// For a file that doesn't already exist, then just fetch it
	//fmt.Printf("Fetching %s, fn:%s\n", fetch_url, fn)
	fetch_file(potential_file_name, dir_str, fetch_url)
	return true
}
