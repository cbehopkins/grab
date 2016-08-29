package grab

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"

	"net/http"

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
	if filename == "" {
		return
	}
	f, err := os.Open(filename)
	if err != nil {
		fmt.Printf("error opening file: %v\n", err)
		os.Exit(1)
		return
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
func crawl(url_in Url, ch UrlChannel, fetch_chan UrlChannel, out_count *OutCounter, errored_urls UrlChannel, print_urls, all_interesting bool) {

	resp, err := http.Get(string(url_in))
	defer out_count.Dec()

	if err != nil {

		fmt.Println("ERROR: Failed to crawl \"" + url_in + "\"")
		DecodeHttpError(err)
		errored_urls <- url_in
		return
	}

	b := resp.Body
	defer b.Close()
	defer io.Copy(ioutil.Discard, b)
	z := html.NewTokenizer(b)
	if print_urls {
		fmt.Printf("Analyzing UR: %s\n", url_in)
	}
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

			// Make sure the url begines in http**
			hasProto := strings.Index(linked_url, "http") == 0
			if hasProto {

				is_jpg := strings.Contains(linked_url, ".jpg")
				if is_jpg {
					out_count.Add()
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
						out_count.Add()
						ch <- Url(linked_url)
					} else {
						if print_urls {
							fmt.Printf("Uninteresting url, %s, %s, %s\n", domain_i, domain_j, linked_url)
						}
					}
				}
			}
		}
	}
}
