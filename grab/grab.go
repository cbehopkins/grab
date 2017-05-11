package grab

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"log"
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
	for s, e := Readln(r); e == nil; s, e = Readln(r) {
		s = strings.TrimSpace(s)
		comment := strings.HasPrefix(s, "//")
		comment = comment || strings.HasPrefix(s, "#")
		if comment {
			continue
		}
		if s == "" {
			continue
		}
		//fmt.Println("Fast adding ", s)
		if inc_count {
			counter.Add()
		}
		tmp_u := NewUrl(s)
		if tmp_u.Initialise() {
			log.Fatal("nneded to init after new")
		}
		the_chan <- tmp_u
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
	defer f.Close()
	fi, err := f.Stat()
	check(err)
	if fi.Size() > 1 {
		//fmt.Printf("Opened filename:%s,%d\n", filename, fi.Size())
		buff := make([]Url, 0)
		dec := gob.NewDecoder(f)
		err = dec.Decode(&buff)
		fmt.Printf("Gobbed in %d Items\n", len(buff))
		check(err)
		for _, v := range buff {
			v.Initialise()
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
	if _, err := os.Stat(filename); err == nil {
		os.Remove(filename)
	}

	preader, pwriter := io.Pipe()

	f, err := os.Create(filename)
	check(err)
	defer f.Close()

	go func() {
		last_read := false
		for !last_read {
			buf := make([]byte, 1<<10)
			//fmt.Println("reding into buffer")
			// REVISIT It would be more //el to read first outside the loop
			// Then we could pass the next buffer to the read while we wrote
			n, err := preader.Read(buf)
			//fmt.Println("Read from buffer",n,err)

			if err == io.EOF {
				fmt.Println("Detected end of file")
				last_read = true
			} else if err != nil {
				panic(err)
			}
			// Reslice so writer has the correct input
			buf = buf[:n]
			len_remaining := n
			for len_remaining > 0 {
				//fmt.Println("writing bytes:",len_remaining, len(buf), buf)
				n, err = f.Write(buf)
				//fmt.Println("Wrote",n,err)
				check(err)
				len_remaining -= n
			}
			if last_read {
				preader.Close()
				//pwriter.Close()
				f.Close()
				fmt.Println("Done and closing everything")
				return
			}
		}
	}()

	buff := make([]Url, 0)
	fmt.Println("Start bufferring")
	for v := range the_chan {
		buff = append(buff, v)
	}
	fmt.Printf("Gobbing Out %d Items\n", len(buff))
	enc := gob.NewEncoder(pwriter)

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
func runChanW(input_chan <-chan Url, visited_urls, unvisit_urls *UrlMap, dbg_name string, wg *sync.WaitGroup) {
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
}
func RunChan(input_chan <-chan Url, visited_urls, unvisit_urls *UrlMap, dbg_name string) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go runChanW(input_chan, visited_urls, unvisit_urls, dbg_name, &wg)
	return &wg
}

func (mf *MultiFetch) FetchW(fetch_url Url) bool {
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
		if mf.jpg_tk == nil {
			// We're not testing all the jpgs for goodness
			//fmt.Println("skipping downloading", potential_file_name)
			return false
		} else if strings.HasSuffix(fn, ".jpg") {
			// Check if it is a corrupted file. If it is, then fetch again
			//fmt.Println("yest jph", fn)
			mf.jpg_tk.GetToken("jpg")
			good_file := check_jpg(potential_file_name)
			mf.jpg_tk.PutToken("jpg")
			if good_file {
				return false
			}
		} else {
			// not a jpg and it already exists
			return false
		}

	}

	// For a file that doesn't already exist, then just fetch it
	//fmt.Printf("Fetching %s, fn:%s\n", fetch_url, fn)
	fetch_file(potential_file_name, dir_str, fetch_url)
	return true
}
