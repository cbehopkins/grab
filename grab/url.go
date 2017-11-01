package grab

import (
	"encoding/json"
	"log"
	"net/url"
	"strings"
)

// URL struct contains fast ways to access the url itself
// and cached versions of the processed and parsed structures
type URL struct {
	URLs        string `json:"-"`
	Title       string `json:"t,omitempty"`
	Promiscuous bool   `json:"p,omitempty"`
	Shallow     bool   `json:"s,omitempty"`
	base        *string
	parse       **url.URL
}

// NewURL creates a new url from a string
// we a lazy with our processing to spread the workload
func NewURL(ur string) (ret URL) {
	ret.URLs = ur
	ret.Initialise()
	return ret
}

// URLChannel is what it says
type URLChannel chan URL

// NewURLChannel create a new URLChannel
func NewURLChannel() *URLChannel {
	var itm URLChannel
	itm = make(URLChannel)
	return &itm
}

// Initialise the URL giving it the underlying structs it needs
func (u *URL) Initialise() bool {
	if u.base == nil || u.parse == nil {
		u.base = new(string)
		u.parse = new(*url.URL)
		return true
	}
	return false
}

// Key abstracts the usage to allow make handling trivial
// This is the function you should call
// if you want to generate a key of the map
func (u URL) Key() string {
	return u.URL()
}

// URL returns  the actual URL itself
func (u URL) URL() string {
	return u.URLs
}

// A strigified version of the URL structure
func (u URL) String() string {
	retStr := "URL:\"" + u.URL() + "\""
	if u.Title != "" {
		retStr += "Title:\"" + u.Title + "\""
	}
	if u.Promiscuous {
		retStr += "Promiscuous"
	}
	if u.Shallow {
		retStr += "Shallow"
	}
	return retStr
}

// ToBa requird as the disk only deals in byte arrays
func (u URL) ToBa() []byte {
	return []byte(u.URLs)
}

// FromBa populates a url from the information in the input byte array
func (u *URL) FromBa(key string, in []byte) {
	if string(in) == "" {
		// Null written to disk
		*u = NewURL(key)
		u.Base()
	} else {
		log.Fatal("This functionality not supported yet")
	}
}

// NewURLFromBa trivially pass in a byte array
func NewURLFromBa(in []byte) URL {
	return NewURL(string(in))
}

// Base - The base of a URL
func (u URL) Base() string {
	if u.URLs == "" {
		return ""
	}
	if *u.parse == nil {
		u.parseUs()

		if *u.base == "" {
			u.genBase()
		}
	}
	return *u.base
}

// SetTitle record the page title of the page we were found on
func (u *URL) SetTitle(tt string) {
	u.Title = tt
}

// GetTitle get the page title of the page we were found on
func (u URL) GetTitle() string {
	return u.Title
}
func (u URL) parseUs() {
	var err error
	if *u.parse == nil && u.URLs != "" {
		*u.parse, err = url.Parse(u.URLs)
		if err != nil || *u.parse == nil {
			u.URLs = ""
			u.Title = ""
			*u.base = ""
		}
	}
}

// Parse the URL into required structure
func (u URL) Parse() *url.URL {
	u.parseUs()
	return *u.parse
}
func (u URL) genBase() {
	hn := u.Parse().Hostname()
	// REVISIT pass this through GoodURL
	if hn == "http" || hn == "nats" || strings.Contains(hn, "+document.location.host+") {
		*u.base = ""
	} else {
		*u.base = hn
	}
}

// GetBase is an exportable way to get the base of the url
func GetBase(urls string) string {
	var u URL
	u.URLs = urls
	u.Initialise()
	//var ai *url.URL
	//var err error
	//ai, err = url.Parse(urls)
	//check(err)
	//hn := ai.Hostname()
	//if hn == "http" || hn == "nats" || strings.Contains(hn, "+document.location.host+") {
	//	return ""
	//} else {
	//	return hn
	//}
	return u.Base()
}

// SetPromiscuous set prom mode
func (u *URL) SetPromiscuous() {
	u.Promiscuous = true
}

// ClearPromiscuous clear prom mode
func (u *URL) ClearPromiscuous() {
	u.Promiscuous = false
}

// GetPromiscuous returns is we are in promiscucous mode
func (u URL) GetPromiscuous() bool {
	return u.Promiscuous
}

// SetShallow set shallow mode
func (u *URL) SetShallow() {
	u.Shallow = true
}

// ClearShallow mode
func (u *URL) ClearShallow() {
	u.Shallow = false
}

// GetShallow returns true if we are in shallow mode
func (u URL) GetShallow() bool {
	return u.Shallow
}
func (u *URL) goMarshalJSON() (output []byte, err error) {
	output, err = json.Marshal(u)
	return
}
func (u *URL) goUnMarshalJSON(input []byte) (err error) {
	err = json.Unmarshal(input, u)
	return
}
