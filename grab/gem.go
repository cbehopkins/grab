package grab

import (
	"log"
	"strings"
)

// Gem holds good members to have
// It is only used within the test harness
type gem map[string]bool

func newGem() gem {
	return make(map[string]bool)
}
func (gm gem) Add(txt string) {
	gm[txt] = true
}
func (gm gem) Visit(txt string) {
	for key, val := range gm {
		if val {
			if strings.Contains(txt, key) {
				gm[key] = false
			}
		}
	}
}
func (gm gem) Close() {
	for key, val := range gm {
		if val {
			log.Fatal("Error didn't visit somewhere we should:", key)
		}
	}
}
func (gm gem) GoodMembersChan() <-chan string {
	retChan := make(chan string)
	go gm.goodMembersChan(retChan)
	return retChan
}
func (gm gem) goodMembersChan(retChan chan<- string) {
	defer close(retChan)
	for key, val := range gm {
		if !val {
			retChan <- key
		}
	}
}
