package grab

import (
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

func (u *Url) Initialise() {
	u.base = new(string)
	u.parse = new(*url.URL)
}

func (u Url) Url() string {
	return u.UrlS
}
func (u Url) String() string {
	return u.Url()
}
func (u Url) Base() string {
	if *u.base == "" {
		u.genBase()
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
