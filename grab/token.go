package grab

import (
	"log"
	"math/rand"
	"time"
)

type ToC chan struct{}
type TokenChan struct {
	tc       ToC
	autofill bool
}

func NewTokenChan(delay int, num_tokens int, name string) *TokenChan {
	var itm TokenChan
	itm.tc = make(ToC, num_tokens)
	if delay > 0 {
		itm.SetAutoFill()
		go itm.token_source(delay, name)
	} else {
		for i := 0; i < num_tokens; i++ {
			itm.PutToken()
		}
	}
	return &itm
}
func (tc *TokenChan) SetAutoFill() {
	tc.autofill = true
}
func (tc TokenChan) GetToken() {
	<-tc.tc
}
func (tc TokenChan) PutToken() {
	if !tc.autofill {
		tc.TryPutToken()
	}
}
func (tc TokenChan) TryPutToken() {
	select {
	case tc.tc <- struct{}{}:
	default:
		if tc.autofill {
			log.Fatal("Cannot return Token")
		}
	}
}
func (tc TokenChan) token_source(sleep int, action string) {
	r := rand.New(rand.NewSource(1))
	for {
		tc.tc <- struct{}{}
		var time_to_sleep time.Duration
		if sleep != 0 {
			time_to_sleep = time.Millisecond * time.Duration(r.Intn(sleep))
		}
		if action != "" {
		log.Printf("It will be %s until next %s\n", time_to_sleep, action)
		}
		time.Sleep(time_to_sleep)
	}
}
