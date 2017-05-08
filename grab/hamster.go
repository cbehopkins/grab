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

	re  *regexp.Regexp
	re1 *regexp.Regexp
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
	print_urls  bool
	robotsCache *RobotCache
}

func NewHamster(promiscuous, shallow, all_interesting, print_urls bool) *Hamster {
	itm := new(Hamster)
	itm.promiscuous = promiscuous
	itm.shallow = shallow
	itm.all_interesting = all_interesting
	itm.print_urls = print_urls
	itm.re = regexp.MustCompile("'http.*'")
	// For our purposes, anything after the ? is an annoyance
	itm.re1 = regexp.MustCompile("'?(.*)\\?")

	return itm
}
func (hm *Hamster) Polite() {
	hm.robotsCache = NewRobotCache()
}

func (hm *Hamster) Copy() *Hamster {
	itm := new(Hamster)
	itm.oc = hm.oc
	itm.dv = hm.dv
	itm.grab_ch = hm.grab_ch
	itm.fetch_chan = hm.fetch_chan
	itm.re = hm.re
	itm.re1 = hm.re1

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
func (hm *Hamster) Close() {
	if hm.robotsCache != nil {
		hm.robotsCache.Close()
	}
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
	if !hm.dv.GoodUrl(url_in) {
		//fmt.Printf("%s is not a Good Url\n", url_in)
		return
	}

	if hm.print_urls {
		fmt.Printf("Analyzing UR: %s\n", url_in)
		defer fmt.Println("Done with URL:", url_in)
	}

	if hm.robotsCache != nil {
		if !hm.robotsCache.AllowUrl(url_in) {
			return
		}
	}

	// Flag saying we should printout
	domain_i := url_in.Base()
	// Add to the list of domains we have visited (and should visit again)
	// this current URL
	//_ = ur.VisitedA(domain_i)
	timeout := time.Duration(5 * time.Second)
	client := http.Client{
		Timeout: timeout,
	}

	//fmt.Println("Getting:", url_in)
	resp, err := client.Get(url_in.Url())

	if err != nil {
		if hm.print_urls {
			fmt.Println("ERROR: Failed to crawl \"" + url_in.Url() + "\"")
		}
		DecodeHttpError(err)
		return
	}

	b := resp.Body
	defer b.Close() // Defer close to after discard
	defer io.Copy(ioutil.Discard, b)
	//fmt.Println("Tokenizing:",url_in)
	z := html.NewTokenizer(b)
	hm.tokenhandle(z, url_in, domain_i)
	//fmt.Println("Tokenized:",url_in)
}
func (hm *Hamster) urlProc(linked_url, url_in Url, domain_i string, title_text string) {
	// I need to get this into an absolute URL again
	base := url_in.Parse()
	u := linked_url.Parse()
  // Need to re-check for ""
  // as Parse will nil the strings if parse errors
  if linked_url.Url() == "" || url_in.Url() == "" || base == nil || u == nil {
    return
  }
  // Re-write an relative URLs
	relinked_url_string := base.ResolveReference(u).String()
	linked_url = NewUrl(relinked_url_string)


	domain_j := linked_url.Base()
	if domain_j == "" {
		//fmt.Println("Unable to get base,", linked_url)
		return
	}

	is_jpg := strings.Contains(relinked_url_string, ".jpg")
	is_mpg := strings.Contains(relinked_url_string, ".mpg")
	is_mp4 := strings.Contains(relinked_url_string, ".mp4")
	is_avi := strings.Contains(relinked_url_string, ".avi")
	switch {
	case is_jpg:
		relinked_url_string = strings.TrimLeft(relinked_url_string, ".jpg")
		if hm.oc != nil {
			hm.oc.Add()
		}
		if hm.print_urls {
			//fmt.Printf("Found jpg:%s\n", relinked_url_string)
		}
		//fmt.Println("sending to fetch")
		if hm.all_interesting || hm.dv.VisitedQ(domain_j) {
			hm.fetch_chan <- NewUrl(relinked_url_string)
			//fmt.Println("sent")
		}

	case is_mpg, is_mp4, is_avi:
		//fmt.Println("MPG found:", linked_url)
		if hm.all_interesting || hm.dv.VisitedQ(domain_j) {
			tmp_ur :=linked_url
			tmp_ur.SetTitle(title_text)
			hm.fetch_chan <- tmp_ur
			//fmt.Println("sent", linked_url)
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
				//fmt.Printf("Send %s grab\n", linked_url)
				hm.grab_ch <- linked_url
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
	url_in Url,
	domain_i string, title_text string) {

	// Extract the href value, if there is one
	ok, linked_url := getHref(t)
	//fmt.Println("Found a linked url:", linked_url)
	//defer fmt.Println("Finsihed with linked url:", linked_url)
	if !ok {
		return
	}
	if linked_url == "" {
		return
	}
	hm.urlProc(NewUrl(linked_url), url_in, domain_i, title_text)
}
func resolveUrl(url_in Url) string {
	base := url_in.Parse()
	return base.String()
}
func (hm *Hamster) scriptProc(t html.Token,
	script_text string,
	url_in Url,
	domain_i string, title_text string) {

	if strings.Contains(script_text, "http") {
		//t0 := script_text
		t0 := hm.re.FindAllString(script_text, -1)

		for _, v := range t0 {
			t1 := hm.re1.FindStringSubmatch(v)
			if len(t1) > 1 {
				linked_url := NewUrl(t1[1])
				if linked_url.Parse().String() != "" {
					//fmt.Println("URL:", linked_url)
					hm.urlProc(linked_url, url_in, domain_i, title_text)
				}
			}
		}
	} else {
		//log.Println("Proccessing text:", script_text)

	}
}

func (hm *Hamster) tokenhandle(z *html.Tokenizer, url_in Url, domain_i string) {
	title_text := ""
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
			isTitle := t.Data == "title"
			if isAnchor {
				//fmt.Println("Found A")
				hm.anchorProc(
					t,
					url_in,
					domain_i,
					title_text)
			} // end Anchor processing
			if isScript {
				//fmt.Println("Found S")
				curr_tok := t
				next_token := z.Next()
				if next_token == html.TextToken {
					n_t := z.Token()
					script_text := n_t.Data
					hm.scriptProc(
						curr_tok,
						script_text,
						url_in,
						domain_i,
						title_text)

				}
			}
			if isTitle {
				//fmt.Println("Found T")
				//curr_tok := t
				next_token := z.Next()
				if next_token == html.TextToken {
					n_t := z.Token()
					title_text = n_t.Data

					//fmt.Println("The title of the webpage is:", title_text)
				}
			}
		} // End switch
	}
}

func (hm *Hamster) GrabIt(urs Url, out_count *OutCounter, crawl_chan *TokenChan) bool {
	token_got := urs.Base()

	if crawl_chan.TryGetToken(token_got) {
		// If we successfully got the token
		if hm.print_urls {
			fmt.Println("Grab:", urs)
		}
		if !hm.Shallow() {
			go func() {
				// Before we can send this, we need to take a copy of hm
				// so that we can parrallelise this
				hmp := hm.Copy()
				hmp.GrabT(urs, // The URL we are tasked with crawling
					token_got,
					crawl_chan,
				)
				//fmt.Println("Grabbed:", urs)
				// we don't want GrabT doing this as in
				// None Nil mode it will also add, which
				//we don't want for this design
				out_count.Dec()
			}()
			return true
		} else {
			hm.GrabT(urs, // The URL we are tasked with crawling
				token_got,
				crawl_chan,
			)
			out_count.Dec()
		}
	} else {
		out_count.Dec()
	}
	return false
}
