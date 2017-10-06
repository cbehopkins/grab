package grab

import (
	"encoding/json"
	"log"
	"net/url"
	"strings"
)

type Url struct {
	UrlS        string `json:"-"`
	Title       string `json:"t,omitempty"`
	Promiscuous bool   `json:"p,omitempty"`
	Shallow     bool   `json:"s,omitempty"`
	base        *string
	parse       **url.URL
}

func NewUrl(ur string) (ret Url) {
	ret.UrlS = ur
	ret.Initialise()
	return ret
}

type UrlChannel chan Url

func NewUrlChannel() *UrlChannel {
	var itm UrlChannel
	itm = make(UrlChannel)
	return &itm
}

func (u *Url) Initialise() bool {
	if u.base == nil || u.parse == nil {
		u.base = new(string)
		u.parse = new(*url.URL)
		return true
	}
	return false
}

// This is the function you should call
// if you want to generate a key of the map
func (u Url) Key() string {
	return u.Url()
}

// Get the actual URL itself
func (u Url) Url() string {
	return u.UrlS
}

// A strigified version of the URL structure
func (u Url) String() string {
	retStr := "Url:\"" + u.Url() + "\""
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

func (v Url) ToBa() []byte {
	return []byte(v.Url())
}

// FromBa populates a url from the information in the input byte array
func (u *Url) FromBa(key string, in []byte) {
	if string(in) == "" {
		// Null written to disk
		*u = NewUrl(key)
		u.Base()
	} else {
		log.Fatal("This functionality not supported yet")
	}
}
func NewUrlFromBa(in []byte) Url {
	return NewUrl(string(in))
}
func (u Url) Base() string {
	if u.UrlS == "" {
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
func (u *Url) SetTitle(tt string) {
	u.Title = tt
}
func (u Url) GetTitle() string {
	return u.Title
}
func (u Url) parseUs() {
	var err error
	if *u.parse == nil && u.UrlS != "" {
		*u.parse, err = url.Parse(u.UrlS)
		if err != nil || *u.parse == nil {
			u.UrlS = ""
			u.Title = ""
			*u.base = ""
		}
	}

}
func (u Url) Parse() *url.URL {
	u.parseUs()
	return *u.parse
}
func (u Url) genBase() {
	hn := u.Parse().Hostname()
	// REVISIT pass this through GoodUrl
	if hn == "http" || hn == "nats" || strings.Contains(hn, "+document.location.host+") {
		*u.base = ""
	} else {
		*u.base = hn
	}
}

func (u *Url) SetPromiscuous() {
	u.Promiscuous = true
}
func (u *Url) ClearPromiscuous() {
	u.Promiscuous = false
}
func (u Url) GetPromiscuous() bool {
	return u.Promiscuous
}

func (u *Url) SetShallow() {
	u.Shallow = true
}
func (u *Url) ClearShallow() {
	u.Shallow = false
}
func (u Url) GetShallow() bool {
	return u.Shallow
}
func (item *Url) goMarshalJSON() (output []byte, err error) {
	output, err = json.Marshal(item)
	return
}
func (item *Url) goUnMarshalJSON(input []byte) (err error) {
	err = json.Unmarshal(input, item)
	return
}
