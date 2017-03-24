package grab

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"net/http"
	"net/url"
	"os"

	"golang.org/x/net/html"
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
	if close_chan {
		close(the_chan)
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
	return ai.Host
}

// Extract all http** links from a given webpage
func GrabT(
	url_in Url, // The URL we are tasked with crawling
	token_name string,
	all_interesting, print_urls bool,
	oc Occer,
	dv DomVisitI,
	ch,
	fetch_chan chan Url,
	crawl_chan *TokenChan,
) {
	// Make sure we delete the counter/marker
	// on the tracker of the nummber of outstanding processes
	if oc != nil {
		defer oc.Dec()
	}
	// And return the crawl token for re-use
	defer crawl_chan.PutToken(token_name)

	if print_urls {
		fmt.Printf("Analyzing UR: %s\n", url_in)
		defer fmt.Println("Done with URL:", url_in)
	}
	// Flag saying we should printout
	ai, err := url.Parse(string(url_in))
	check(err)
	domain_i := ai.Host
	// Add to the list of domains we have visited (and should visit again)
	// this current URL
	//_ = ur.VisitedA(domain_i)
	timeout := time.Duration(5 * time.Second)
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Get(string(url_in))

	if err != nil {

		//fmt.Println("ERROR: Failed to crawl \"" + url_in + "\"")
		DecodeHttpError(err)
		return
	}

	b := resp.Body
	defer b.Close() // Defer close to after discard
	defer io.Copy(ioutil.Discard, b)
	z := html.NewTokenizer(b)
	tokenhandle(z, string(url_in), domain_i,
		all_interesting, print_urls,
		oc,
		dv,
		ch,
		fetch_chan)
}

func tokenhandle(z *html.Tokenizer, url_in, domain_i string,
	all_interesting, print_urls bool,
	oc Occer,
	dv DomVisitI,
	ch,
	fetch_chan chan Url) {
	for {
		tt := z.Next()

		//fmt.Println("Processing token")

		switch {
		case tt == html.ErrorToken:
			// End of the document, we're done
			return
		case tt == html.StartTagToken:
			t := z.Token()
			//fmt.Println("Start Token")
			// Check if the token is an <a> tag
			isAnchor := t.Data == "a"
			if !isAnchor {
				continue
			}

			// Extract the href value, if there is one
			ok, linked_url := getHref(t)
			if !ok {
				continue
			}

			// I need to get this into an absolute URL again
			base, err := url.Parse(string(url_in))
			check(err)
			u, err := url.Parse(linked_url)
			if err != nil {
				continue
			}
			linked_url_ut := base.ResolveReference(u)
			linked_url_string := linked_url_ut.String()
			linked_url = linked_url_string
			if print_urls {
				//fmt.Println("Resolved URL to:", linked_url)
			}

			is_jpg := strings.Contains(linked_url, ".jpg")
			if is_jpg {
				if oc != nil {
					oc.Add()
				}
				if print_urls {
					//fmt.Printf("Found jpg:%s\n", linked_url)
				}
				//fmt.Println("sending to fetch")
				fetch_chan <- Url(linked_url)
				//fmt.Println("sent")
			} else {

				aj, err := url.Parse(string(linked_url))
				check(err)

				domain_j := aj.Host
				if all_interesting || dv.VisitedQ(domain_j) || (domain_i == domain_j) {
					if print_urls {
						//fmt.Printf("Interesting url, %s, %s, %s\n", domain_i, domain_j, linked_url)
					}
					if oc != nil {
						oc.Add()
					}
					//fmt.Printf("Send %s grab\n",linked_url)
					ch <- Url(linked_url)
					//fmt.Println("Sent %s to grab\n",linked_url)
				} else {
					if print_urls {
						//fmt.Printf("Uninteresting url, %s, %s, %s\n", domain_i, domain_j, linked_url)
					}
				}
			}
			//}
		} // End switch
	}
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
