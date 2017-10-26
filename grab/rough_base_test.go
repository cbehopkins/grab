package grab

import (
	"log"
	"os"
	"testing"
)

func (st *DkCollection) checkBase(url, base string) bool {
	if st.roughBase(url) == base {
		return true
	} else {
		return false
	}
}
func TestCheckBase0(t *testing.T) {
	st := NewDkCollection(os.TempDir()+"/bob", true)
	st.checkBase("http://play.google.com", "play.google.com")
}
func TestCheckBase1(t *testing.T) {
	st := NewDkCollection(os.TempDir()+"/bob", true)
	test_strings := []string{
		"http://play.google.com",
		"https://play.google.com",
		"http://www.fred.com/bob",
		"http://www.fred.com/bob/dave/",
		"https://fred.com/bob",
	}
	for _, v := range test_strings {
		if !st.checkBase(st.roughBase(v), GetBase(v)) {
			log.Fatalf("\nFor URL:%s\nRef Base:%v\nOurBase:%v", v, GetBase(v), st.roughBase(v))
		}
	}
}
