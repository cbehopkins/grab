package grab

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"regexp"
	"strings"
	"time"

	"net/http"
	"net/url"

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
	itm.dv = hm.dv
	itm.fetchChan = hm.fetchChan
	itm.re = hm.re
	itm.re1 = hm.re1

	itm.promiscuous = hm.promiscuous
	itm.allInteresting = hm.allInteresting
	itm.printUrls = hm.printUrls
	return itm
}

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

// Run the grab process
// and we have got the token to do this
func (hm *Hamster) grabWithToken(
	urlIn URL, // The URL we are tasked with crawling
	tokenName string,
	crawlChan *TokenChan,
	grabChan chan<- URL,
) {
	// And return the crawl token for re-use
	defer crawlChan.PutToken(tokenName)
	if !hm.dv.GoodURL(urlIn) {
		//fmt.Printf("%s is not a Good URL\n", url_in)
		return
	}

	//if hm.printUrls {
	//	fmt.Printf("Analyzing UR: %s\n", urlIn)
	//	defer fmt.Println("Done with URL:", urlIn)
	//}

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
func (hm *Hamster) urlProc(urlOrig, urlIn URL, domainI string, grabChan chan<- URL) {
	// Re-write an relative URLs
	relinkedURLString := urlIn.String()
	linkedURL := urlIn

	domainJ := linkedURL.Base()
	if domainJ == "" {
		log.Println("Unable to get base, in urlProc", linkedURL)
		return
	}
	//fmt.Println("urlProc on:",urlIn)
	isJpg := strings.Contains(relinkedURLString, ".jpg")
	isMpg := strings.Contains(relinkedURLString, ".mpg")
	isMp4 := strings.Contains(relinkedURLString, ".mp4")
	isAvi := strings.Contains(relinkedURLString, ".avi")
	isGz := strings.Contains(relinkedURLString, ".gz")
	switch {
	case isJpg:
		//if hm.printUrls {
		log.Printf("Found jpg:%s\n", relinkedURLString)
		//}
		if hm.allInteresting || hm.dv.VisitedQ(domainJ) {
			hm.fetchChan <- linkedURL
		}

	case isMpg, isMp4, isAvi:
		//if hm.printUrls {
		log.Println("MPG found:", linkedURL)
		//}
		vqok := hm.dv.VisitedQ(domainJ)
		if hm.allInteresting || vqok {
			if vqok {
				tmpUr := linkedURL
				hm.fetchChan <- tmpUr
			} else {
				if hm.printUrls {
					fmt.Println(domainJ, " not allowed")
				}
			}
		}
	case isGz:
	default:
		grabAllowed := hm.promiscuous || urlOrig.GetPromiscuous() || urlOrig.GetShallow()
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
				grabChan <- linkedURL

				if hm.printUrls {
					fmt.Printf("Uninteresting url, %s, %s, %s\n", domainI, domainJ, linkedURL)
				}
			}
		}
	}
}

func (hm *Hamster) foundUrl(urlOrig, url URL, domainI string, grabChan chan<- URL) {

	if url.String() != "" {
		hm.urlProc(urlOrig, url, domainI, grabChan)
	}
}

func (hm *Hamster) anchorProc(
	urlIn URL,
	domainI, titleText string,
	t html.Token,
	grabChan chan<- URL,
) {
	// Extract the href value, if there is one
	//fmt.Println("Anchor running with", urlIn)
	ok, linkedURL := getHref(t)
	if !ok {
		return
	}
	if linkedURL == "" {
		return
	}
	//fmt.Println("Link is:", linkedURL)
	urn := hm.relativeLink(urlIn, linkedURL)
	urn.SetTitle(titleText)
	hm.foundUrl(urlIn, urn, domainI, grabChan)

}
func (hm *Hamster) relativeLink(urlIn URL, linkedURL string) URL {
	urS := urlIn.Parse()
	tu, err := url.Parse(linkedURL)
	if err != nil {
		return NewURL("")
	}
	if tu.IsAbs() {
		return NewURLFromParse(tu)
	}
	urn := urS.ResolveReference(tu)
	return NewURLFromParse(urn)
}

func (hm *Hamster) scriptProc(
	urlIn URL,
	domainI, titleText string,
	scriptText string,
	grabChan chan<- URL,
) {

	if strings.Contains(scriptText, "http") {
		t0 := hm.re.FindAllString(scriptText, -1)

		for _, v := range t0 {
			t1 := hm.re1.FindStringSubmatch(v)
			if len(t1) > 1 {
				linkedURL := t1[1]
				urn := hm.relativeLink(urlIn, linkedURL)
				urn.SetTitle(titleText)
				hm.foundUrl(urlIn, urn, domainI, grabChan)
			}
		}
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
			// fmt.Println("Start Token")
			// Check if the token is an <a> tag
			isAnchor := t.Data == "a"
			isScript := t.Data == "script"
			isTitle := t.Data == "title"
			if isAnchor {
				hm.anchorProc(
					urlIn,
					domainI,
					titleText,
					t,
					grabChan,
				)
			} // end Anchor processing
			if isScript {
				nextToken := z.Next()
				if nextToken == html.TextToken {
					nT := z.Token()
					scriptText := nT.Data
					hm.scriptProc(
						urlIn,
						domainI,
						titleText,
						scriptText,
						grabChan,
					)
				}
			}
			if isTitle {
				//_ = z.Next()
				nextToken := z.Next()
				if nextToken == html.TextToken {
					nT := z.Token()
					titleText = nT.Data
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
				hm.grabWithToken(
					urs, // The URL we are tasked with crawling
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
