package grab

import (
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strings"
	"time"

	"net/http"

	"golang.org/x/net/html"
)

const (
	// UseParallelGrab says that we want many grabbers if possible
	// if possible defined by unique domains
	UseParallelGrab = true 
)

// Hamster is our struture for the the things that grabs and prepares things for storing
// picture a little hamster going exploring and stuffing interesting things into its cheeks for later processing
type Hamster struct {
	// Output counter interface
	oc *OutCounter
	// The Domains we are allowed to visit
	dv DomVisitI
	// Channel we should send Further Urls to grab to
	fetchChan chan<- URL

	re  *regexp.Regexp
	re1 *regexp.Regexp
	// On grab URL should we get everything we possibly can
	promiscuous bool
	// A shallow fetch is one where the first fetch gets all
	// Urls from the first page
	// Then Fetches everything from those Urls
	// Then stops
	shallow bool
	// All Urls we find are interesting
	// vs they need to be from an allowed domain
	allInteresting bool
	// Debug
	printUrls   bool
	robotsCache *RobotCache
}

// NewHamster returns a new hamster
func NewHamster(promiscuous, allInteresting, printUrls, polite bool) *Hamster {
	itm := new(Hamster)
	itm.promiscuous = promiscuous

	itm.allInteresting = allInteresting
	itm.printUrls = printUrls
	itm.re = regexp.MustCompile("'http.*'")
	// For our purposes, anything after the ? is an annoyance
	itm.re1 = regexp.MustCompile("'?(.*)\\?")
	if polite {
		itm.Polite()
	}
	return itm
}

// Polite says this should be a polite hamster
// i.e. respect robots.txt
func (hm *Hamster) Polite() {
	hm.robotsCache = NewRobotCache()
}

// Duplicate one Hamster function into anoter
func (hm *Hamster) Duplicate() *Hamster {
	itm := new(Hamster)
	itm.oc = hm.oc
	itm.dv = hm.dv
	itm.fetchChan = hm.fetchChan
	itm.re = hm.re
	itm.re1 = hm.re1

	itm.promiscuous = hm.promiscuous
	itm.allInteresting = hm.allInteresting
	itm.printUrls = hm.printUrls
	return itm
}

// Set the Out Counter to work on for this hamster
//func (hm *Hamster) SetOc(oc *OutCounter) {
//	hm.oc = oc
//}

// SetDv Sets the Domain Visit structure to use
func (hm *Hamster) SetDv(dv DomVisitI) {
	hm.dv = dv
}

// SetFetchCh sets the channel we should put anything we think we should fetch
func (hm *Hamster) SetFetchCh(fetchChan chan<- URL) {
	hm.fetchChan = fetchChan
}

// Close down the Hamster
func (hm *Hamster) Close() {
	if hm.robotsCache != nil {
		hm.robotsCache.Close()
	}
}
func (hm *Hamster) grabWithToken(
	urlIn URL, // The URL we are tasked with crawling
	tokenName string,
	crawlChan *TokenChan,
	grabChan chan<- URL,
) {
	// Make sure we delete the counter/marker
	// on the tracker of the nummber of outstanding processes
	if hm.oc != nil {
		defer hm.oc.Dec()
	}
	// And return the crawl token for re-use
	defer crawlChan.PutToken(tokenName)
	if !hm.dv.GoodURL(urlIn) {
		//fmt.Printf("%s is not a Good URL\n", url_in)
		return
	}

	if hm.printUrls {
		fmt.Printf("Analyzing UR: %s\n", urlIn)
		defer fmt.Println("Done with URL:", urlIn)
	}

	if hm.robotsCache != nil {
		if !hm.robotsCache.AllowURL(urlIn) {
			return
		}
	}

	// Add to the list of domains we have visited (and should visit again)
	// this current URL
	//_ = ur.VisitedA(domain_i)
	timeout := time.Duration(GrabTimeout)
	client := http.Client{
		Timeout: timeout,
	}

	resp, err := client.Get(urlIn.URL())
	if err != nil {
		if hm.printUrls {
			fmt.Println("ERROR: Failed to crawl \"" + urlIn.URL() + "\"")
		}
		DecodeHTTPError(err)
		return
	}

	b := resp.Body
	defer b.Close() // Defer close to after discard
	defer io.Copy(ioutil.Discard, b)
	z := html.NewTokenizer(b)
	domainI := urlIn.Base()
	hm.tokenhandle(z, urlIn, domainI, grabChan)
}
func (hm *Hamster) urlProc(linkedURL, urlIn URL, domainI string, titleText string, grabChan chan<- URL) {
	// I need to get this into an absolute URL again
	base := urlIn.Parse()
	u := linkedURL.Parse()
	// Need to re-check for ""
	// as Parse will nil the strings if parse errors
	if linkedURL.URL() == "" || urlIn.URL() == "" || base == nil || u == nil {
		return
	}
	// Re-write an relative URLs
	relinkedURLString := base.ResolveReference(u).String()
	linkedURL = NewURL(relinkedURLString)

	domainJ := linkedURL.Base()
	if domainJ == "" {
		//fmt.Println("Unable to get base,", linked_url)
		return
	}

	isJpg := strings.Contains(relinkedURLString, ".jpg")
	isMpg := strings.Contains(relinkedURLString, ".mpg")
	isMp4 := strings.Contains(relinkedURLString, ".mp4")
	isAvi := strings.Contains(relinkedURLString, ".avi")
	switch {
	case isJpg:
		relinkedURLString = strings.TrimLeft(relinkedURLString, ".jpg")
		if hm.oc != nil {
			hm.oc.Add()
		}
		if hm.printUrls {
			//fmt.Printf("Found jpg:%s\n", relinked_url_string)
		}
		//fmt.Println("sending to fetch")
		if hm.allInteresting || hm.dv.VisitedQ(domainJ) {
			hm.fetchChan <- NewURL(relinkedURLString)
			//fmt.Println("sent")
		}

	case isMpg, isMp4, isAvi:
		//fmt.Println("MPG found:", linked_url)
		if hm.allInteresting || hm.dv.VisitedQ(domainJ) {
			tmpUr := linkedURL
			tmpUr.SetTitle(titleText)
			hm.fetchChan <- tmpUr
			//fmt.Println("sent", linked_url)
		}
	default:
		grabAllowed := hm.promiscuous || urlIn.GetPromiscuous() || urlIn.GetShallow()
		if hm.promiscuous {
			linkedURL.SetPromiscuous()
		}
		if grabAllowed {
			interestingURL := hm.allInteresting || hm.dv.VisitedQ(domainJ) || (domainI == domainJ)
			if interestingURL {
				if hm.printUrls {
					fmt.Printf("Interesting url, %s, %s\n", linkedURL, urlIn)
				}
				if !hm.dv.GoodURL(urlIn) {
					return
				}
				if hm.oc != nil {
					hm.oc.Add()
				}
				//fmt.Printf("Send %s grab\n", linked_url)
				grabChan <- linkedURL
				//fmt.Println("Sent %s to grab\n",linked_url)

				if hm.printUrls {
					//fmt.Printf("Uninteresting url, %s, %s, %s\n", domainI, domainJ, linkedURL)
				}
			}
		}
	}
	//}

}
func (hm *Hamster) anchorProc(t html.Token,
	urlIn URL,
	domainI string, titleText string,
	grabChan chan<- URL,
) {

	// Extract the href value, if there is one
	ok, linkedURL := getHref(t)
	if !ok {
		return
	}
	if linkedURL == "" {
		return
	}
	hm.urlProc(NewURL(linkedURL), urlIn, domainI, titleText, grabChan)
}

//func resolveUrl(url_in URL) string {
//	base := url_in.Parse()
//	return base.String()
//}
func (hm *Hamster) scriptProc(t html.Token,
	scriptText string,
	urlIn URL,
	domainI string, titleText string,
	grabChan chan<- URL,
) {

	if strings.Contains(scriptText, "http") {
		//t0 := script_text
		t0 := hm.re.FindAllString(scriptText, -1)

		for _, v := range t0 {
			t1 := hm.re1.FindStringSubmatch(v)
			if len(t1) > 1 {
				linkedURL := NewURL(t1[1])
				linkedURL.Parse()
				if linkedURL.String() != "" {
					//fmt.Println("URL:", linked_url)
					hm.urlProc(linkedURL, urlIn, domainI, titleText, grabChan)
				}
			}
		}
	} else {
		//log.Println("Proccessing text:", script_text)

	}
}

func (hm *Hamster) tokenhandle(z *html.Tokenizer, urlIn URL, domainI string,
	grabChan chan<- URL,
) {
	titleText := ""
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
					urlIn,
					domainI,
					titleText,
					grabChan,
				)
			} // end Anchor processing
			if isScript {
				//fmt.Println("Found S")
				currTok := t
				nextToken := z.Next()
				if nextToken == html.TextToken {
					nT := z.Token()
					scriptText := nT.Data
					hm.scriptProc(
						currTok,
						scriptText,
						urlIn,
						domainI,
						titleText,
						grabChan,
					)
				}
			}
			if isTitle {
				//fmt.Println("Found T")
				//curr_tok := t
				nextToken := z.Next()
				if nextToken == html.TextToken {
					nT := z.Token()
					titleText = nT.Data

					//fmt.Println("The title of the webpage is:", title_text)
				}
			}
		} // End switch
	}
}

func (hm *Hamster) grabItWork(urs URL, outCount *OutCounter, crawlChan *TokenChan, tmpChan chan<- URL) bool {
	tokenGot := urs.Base()

	if crawlChan.TryGetToken(tokenGot) {
		// If we successfully got the token
		if hm.printUrls {
			fmt.Println("grab:", urs)
		}
		if UseParallelGrab {
			outCount.Add()

			go func() {
				// Before we can send this, we need to take a copy of hm
				// so that we can parrallelise this
				hmp := hm.Duplicate()
				hmp.grabWithToken(urs, // The URL we are tasked with crawling
					tokenGot,
					crawlChan,
					tmpChan,
				)
				if hm.printUrls {
					fmt.Println("Grabbed:", urs)
				}
				// we don't want grabWithToken doing this as in
				// None Nil mode it will also add, which
				//we don't want for this design
				outCount.Dec()
			}()
		} else {
			hm.grabWithToken(urs, // The URL we are tasked with crawling
				tokenGot,
				crawlChan,
				tmpChan,
			)
		}
		return true
	}
	return false
}
