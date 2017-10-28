package grab

import (
	"log"
	"os"
	"testing"
)

func (dc *DkCollection) checkBase(url, base string) bool {
	if dc.roughBase(url) == base {
		return true
	}
	return false
}
func TestCheckBase0(t *testing.T) {
	st := NewDkCollection(os.TempDir()+"/bob", true)
	st.checkBase("http://play.google.com", "play.google.com")
}
func TestCheckBase1(t *testing.T) {
	st := NewDkCollection(os.TempDir()+"/bob", true)
	testStrings := []string{
		"http://play.google.com",
		"https://play.google.com",
		"http://www.fred.com/bob",
		"http://www.fred.com/bob/dave/",
		"https://fred.com/bob",
	}
	for _, v := range testStrings {
		if !st.checkBase(st.roughBase(v), GetBase(v)) {
			log.Fatalf("\nFor URL:%s\nRef Base:%v\nOurBase:%v", v, GetBase(v), st.roughBase(v))
		}
	}
}
