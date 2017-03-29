package grab

import (
	"fmt"
	"io"
	"io/ioutil"
	//"log"
	"regexp"
	"strings"
	"time"

	"net/http"
	"net/url"

	"golang.org/x/net/html"
)

// A hamster is our struture for the the things that grabs and prepares things for storing
type Hamster struct {
	// Output counter interface
	oc *OutCounter
	// The Domains we are allowed to visit
	dv DomVisitI
	// Channel we should send Further Urls to grab to
	grab_ch    chan Url
	fetch_chan chan Url
	// On Grab URL should we get everything we possibly can
	promiscuous bool
	// A shallow fetch is one where the first fetch gets all
	// Urls from the first page
	// Then Fetches everything from those Urls
	// Then stops
	shallow bool
	// All Urls we find are interesting
	// vs they need to be from an allowed domain
	all_interesting bool
	// Debug
	print_urls bool
}

func NewHamster(promiscuous, shallow, all_interesting, print_urls bool) *Hamster {
	itm := new(Hamster)
	itm.promiscuous = promiscuous
	itm.shallow = shallow
	itm.all_interesting = all_interesting
	itm.print_urls = print_urls
	return itm
}
func (hm *Hamster) Copy() *Hamster {
	itm := new(Hamster)
	itm.oc = hm.oc
	itm.dv = hm.dv
	itm.grab_ch = hm.grab_ch
	itm.fetch_chan = hm.fetch_chan

	itm.promiscuous = hm.promiscuous
	itm.shallow = hm.shallow
	itm.all_interesting = hm.all_interesting
	itm.print_urls = hm.print_urls
	return itm
}

func (hm *Hamster) SetOc(oc *OutCounter) {
	hm.oc = oc
}
func (hm *Hamster) SetDv(dv DomVisitI) {
	hm.dv = dv
}
func (hm *Hamster) SetGrabCh(grab_ch chan Url) {
	hm.grab_ch = grab_ch
}
func (hm *Hamster) SetFetchCh(fetch_chan chan Url) {
	hm.fetch_chan = fetch_chan
}
func (hm Hamster) Shallow() bool {
	return hm.shallow
}
func (hm *Hamster) SetShallow() {
	hm.shallow = true
}
func (hm *Hamster) ClearShallow() {
	hm.shallow = false
}
func (hm *Hamster) GrabT(
	url_in Url, // The URL we are tasked with crawling
	token_name string,
	crawl_chan *TokenChan,
) {
	// Make sure we delete the counter/marker
	// on the tracker of the nummber of outstanding processes
	if hm.oc != nil {
		defer hm.oc.Dec()
	}
	// And return the crawl token for re-use
	defer crawl_chan.PutToken(token_name)
	if !hm.dv.GoodUrl(string(url_in)) {
		fmt.Printf("%s is not a Good Url\n", url_in)
		return
	}

	if hm.print_urls {
		fmt.Printf("Analyzing %v,UR: %s\n", hm.shallow, url_in)
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
		fmt.Println("ERROR: Failed to crawl \"" + url_in + "\"")
		DecodeHttpError(err)
		return
	}

	b := resp.Body
	defer b.Close() // Defer close to after discard
	defer io.Copy(ioutil.Discard, b)
	z := html.NewTokenizer(b)
	hm.tokenhandle(z, string(url_in), domain_i)
	// After we have done the first page, then turn off shallow
	// i.e. fetch no new Urls
	hm.shallow = false
}
func (hm *Hamster) urlProc(linked_url, url_in, domain_i string) {
	// I need to get this into an absolute URL again
	base, err := url.Parse(string(url_in))
	check(err)
	u, err := url.Parse(linked_url)
	if err != nil {
		return
	}
	linked_url_string := base.ResolveReference(u).String()
	linked_url = linked_url_string
	if hm.print_urls {
		fmt.Println("Resolved URL to:", linked_url)
	}
	domain_j := GetBase(linked_url)
	if domain_j == "" {
		return
	}

	is_jpg := strings.Contains(linked_url, ".jpg")
	is_mpg := strings.Contains(linked_url, ".mpg")
	is_mp4 := strings.Contains(linked_url, ".mp4")
	is_avi := strings.Contains(linked_url, ".avi")
	switch {
	case is_jpg:
		linked_url = strings.TrimLeft(linked_url, ".jpg")
		if hm.oc != nil {
			hm.oc.Add()
		}
		if hm.print_urls {
			//fmt.Printf("Found jpg:%s\n", linked_url)
		}
		//fmt.Println("sending to fetch")
		if hm.all_interesting || hm.dv.VisitedQ(domain_j) {
			hm.fetch_chan <- Url(linked_url)
			//fmt.Println("sent")
		}

	case is_mpg, is_mp4, is_avi:
		if is_mpg {
			linked_url = strings.TrimLeft(linked_url, ".mpg")
		}
		if is_mp4 {
			linked_url = strings.TrimLeft(linked_url, ".mp4")
		}
		if is_avi {
			linked_url = strings.TrimLeft(linked_url, ".avi")
		}
		if hm.all_interesting || hm.dv.VisitedQ(domain_j) {
			hm.fetch_chan <- Url(linked_url)
			fmt.Println("sent", linked_url)
		}
	default:
		if hm.promiscuous || hm.shallow {
			if hm.all_interesting || hm.dv.VisitedQ(domain_j) || (domain_i == domain_j) {
				if hm.print_urls {
					fmt.Printf("Interesting url, %s, %s, %s, %s\n", domain_i, domain_j, linked_url, url_in)
				}
				if !hm.dv.GoodUrl(url_in) {
					return
				}
				if hm.oc != nil {
					hm.oc.Add()
				}
				//fmt.Printf("Send %s grab\n",linked_url)
				hm.grab_ch <- Url(linked_url)
				//fmt.Println("Sent %s to grab\n",linked_url)

				if hm.print_urls {
					//fmt.Printf("Uninteresting url, %s, %s, %s\n", domain_i, domain_j, linked_url)
				}
			}
		}
	}
	//}

}
func (hm *Hamster) anchorProc(t html.Token,
	url_in string,
	domain_i string) {

	// Extract the href value, if there is one
	ok, linked_url := getHref(t)
	if !ok {
		return
	}
	if linked_url == "" {
		return
	}
	hm.urlProc(linked_url, url_in, domain_i)

}
func resolveUrl(url_in string) string {
	base, err := url.Parse(string(url_in))
	if err != nil {
		return ""
	}
	return base.String()
}
func (hm *Hamster) scriptProc(t html.Token,
	script_text string,
	url_in string,
	domain_i string) {

	if strings.Contains(script_text, "http") {
		re := regexp.MustCompile("'http.*'")
		// For our purposes, anything after the ? is an annoyance
		re1 := regexp.MustCompile("'?(.*)\\?")
		//t0 := script_text
		t0 := re.FindAllString(script_text, -1)

		for _, v := range t0 {
			t1 := re1.FindStringSubmatch(v)
			if len(t1) > 1 {
				linked_url := resolveUrl(t1[1])
				if linked_url != "" {
					//fmt.Println("URL:", linked_url)
					hm.urlProc(linked_url, url_in, domain_i)
				}
			}
		}
	} else {
		//log.Println("Proccessing text:", script_text)

	}
}

func (hm *Hamster) tokenhandle(z *html.Tokenizer, url_in, domain_i string) {
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
			isScript := t.Data == "script"

			if isAnchor {
				hm.anchorProc(
					t,
					url_in,
					domain_i)
			} // end Anchor processing
			if isScript {
				curr_tok := t
				next_token := z.Next()
				if next_token == html.TextToken {
					n_t := z.Token()
					script_text := n_t.Data
					hm.scriptProc(
						curr_tok,
						script_text,
						url_in,
						domain_i)

				}
			}
		} // End switch
	}
}