package grab

import "strings"

func (st *DkCollection) roughBase(url string) string {
	//url_back := url
	if strings.HasPrefix(url, "http://") {
		url = url[7:]
	}
	if strings.HasPrefix(url, "https://") {
		url = url[8:]
	}
	loc := strings.Index(url, "/")
	if loc > 0 {
		url = url[:loc]
	}
	if len(url) < 3 {
		//url = GetBase(url_back)
		//url = oldRoughBase(url)
	}
	return url
}
func (st *DkCollection) oldRoughBase(url string) string {
	t1 := st.re.FindStringSubmatch(url)
	if len(t1) > 2 {
		return t1[2]
	} else {
		return url
	}
}
