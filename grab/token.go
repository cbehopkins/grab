package grab

import (
	"fmt"
	"math/rand"
	"time"
)

type TokenChan chan struct{}

func NewTokenChan(delay int, num_tokens int, name string) *TokenChan {
	itm := make(TokenChan, num_tokens)
	if delay > 0 {
		go token_source(delay, itm, name)
	} else {
		for i := 0; i < num_tokens; i++ {
			itm.PutToken()
		}
	}
	return &itm
}
func (tc TokenChan) GetToken() {
	<-tc
}
func (tc TokenChan) PutToken() {
	tc <- struct{}{}
}

func token_source(sleep int, token_chan TokenChan, action string) {
	r := rand.New(rand.NewSource(1))
	for {
		token_chan.PutToken()
		var time_to_sleep time.Duration
		if sleep != 0 {
			time_to_sleep = time.Millisecond * time.Duration(r.Intn(sleep))
		}
		if action != "" {
			fmt.Printf("It will be %s until next %s\n", time_to_sleep, action)
		}
		time.Sleep(time_to_sleep)
	}
}
