package grab

import (
	"bufio"
	"fmt"
	"golang.org/x/net/html"
	"net/url"
	"os"
	"strings"
)

type Url string
type UrlChannel chan Url

func NewUrlChannel() *UrlChannel {
	var itm UrlChannel
	itm = make(UrlChannel)
	return &itm
}

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
		the_chan <- Url(s)
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
		fmt.Fprintf(f, "%s\n", v)

	}
}
func GetBase(urls string) string {
	ai, err := url.Parse(urls)
	check(err)
	hn := ai.Hostname()
	if hn == "http" || hn == "nats" || strings.Contains(hn, "+document.location.host+") {
		return ""
	} else {
		return hn
	}
}

// Extract all http** links from a given webpage
func GrabT(
	url_in Url, // The URL we are tasked with crawling
	token_name string,
	hm *Hamster,
	crawl_chan *TokenChan,
) {
	hm.GrabT(url_in, token_name, crawl_chan)
}
func FetchW(fetch_url Url, test_jpg bool) bool {
	// We retun true if we have used network bandwidth.
	// If we have not then it's okay to jump straight onto the next file
	array := strings.Split(string(fetch_url), "/")

	var fn string
	if len(array) > 2 {
		fn = array[len(array)-1]
	} else {
		return false
	}
	if strings.HasPrefix(string(fetch_url), "file") {
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
	fn = strings.Replace(fn, "%", "_", -1)
	fn = strings.Replace(fn, "&", "_", -1)
	fn = strings.Replace(fn, "?", "_", -1)
	fn = strings.Replace(fn, "=", "_", -1)
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
