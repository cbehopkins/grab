package grab

import (
	"log"
	"net/url"
	"strings"
)

type Url struct {
	UrlS  string
	Title string
	base  *string
	parse **url.URL
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
	return u.Url()
}

var UseTags = false

func (v Url) ToBa() []byte {
	if UseTags {
		return []byte("Url:" + v.Url())
	} else {
		return []byte(v.Url())
	}
}
func NewUrlFromBa(in []byte) Url {
	if UseTags {
		str := string(in)
		if strings.HasPrefix(str, "URL:") {
			return NewUrl(str[4:])
		}
		log.Fatal("Impossible Structure:", str)
		return NewUrl(str)
	} else {
		return NewUrl(string(in))
	}
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
