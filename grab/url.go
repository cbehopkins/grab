package grab

type Url struct {
	UrlS  string
	Title string
}

func NewUrl(ur string) (ret Url) {
	ret.UrlS = ur
	return ret
}

type UrlChannel chan Url

func NewUrlChannel() *UrlChannel {
	var itm UrlChannel
	itm = make(UrlChannel)
	return &itm
}
func (u Url) Url() string {
	return u.UrlS
}
func (u Url) String() string {
	return u.Url()
}
func (u Url) Base() string {
	return GetBase(u.Url())
}
func (u *Url) SetTitle(tt string) {
	u.Title = tt
}
func (u Url) GetTitle() string {
	return u.Title
}
