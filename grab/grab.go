package grab

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"

	"net/http"
	"net/url"
	"os"
	"strings"

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

func LoadFile(filename string, the_chan chan Url, counter *OutCounter) {
	defer counter.Dec()
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
	r := bufio.NewReader(f)
	s, e := Readln(r)
	for e == nil {
		//fmt.Println("Fast adding ", s)
		if counter != nil {
			counter.Add()
		}
		the_chan <- Url(s)
		s, e = Readln(r)
	}
}

// Extract all http** links from a given webpage
func (ur UrlRx) crawl(
	url_in Url, // The URL we are tasked with crawling
	errored_urls UrlChannel, // Report errors here
) {
	// Flag saying we should printout
	print_urls := ur.DbgUrls
	all_interesting := ur.AllInteresting
	ch := ur.chUrls             // Any interesting URLs are sent here
	fetch_chan := ur.chan_fetch // Any files to fetch are requested here
	resp, err := http.Get(string(url_in))
	defer ur.OutCount.Dec()
	if print_urls {
		fmt.Printf("Analyzing UR: %s\n", url_in)
		defer fmt.Println("Done with URL:", url_in)
	}
	if err != nil {

		fmt.Println("ERROR: Failed to crawl \"" + url_in + "\"")
		DecodeHttpError(err)
		errored_urls <- url_in
		return
	}

	b := resp.Body
	defer b.Close() // Defer close to after discard
	defer io.Copy(ioutil.Discard, b)
	defer ur.crawl_chan.PutToken()
	z := html.NewTokenizer(b)
	for {
		tt := z.Next()

		switch {
		case tt == html.ErrorToken:
			// End of the document, we're done
			return
		case tt == html.StartTagToken:
			t := z.Token()

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
			check(err)
			linked_url_ut := base.ResolveReference(u)
			linked_url_string := linked_url_ut.String()
			linked_url = linked_url_string
			if print_urls {
				fmt.Println("Resolved URL to:", linked_url)
			}

			is_jpg := strings.Contains(linked_url, ".jpg")
			if is_jpg {
				ur.OutCount.Add()
				if print_urls {
					fmt.Printf("Found jpg:%s\n", linked_url)
				}
				fetch_chan <- Url(linked_url)
			} else {
				arrayi := strings.Split(string(url_in), "/")
				arrayj := strings.Split(string(linked_url), "/")
				domain_i := arrayi[2]
				domain_j := arrayj[2]
				if all_interesting || (domain_i == domain_j) {
					if print_urls {
						fmt.Printf("Interesting url, %s, %s, %s\n", domain_i, domain_j, linked_url)
					}
					ur.OutCount.Add()
					ch <- Url(linked_url)
				} else {
					if print_urls {
						fmt.Printf("Uninteresting url, %s, %s, %s\n", domain_i, domain_j, linked_url)
					}
				}
			}
			//}
		}
	}
}
