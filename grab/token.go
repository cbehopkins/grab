package grab

import (
	"log"
	"sync"
)

type ToC chan struct{}
type TokenChan struct {
	sync.Mutex
	open_tokens    map[string]int
	broadcast_chan chan struct{}
	name           string
	max_cnt        int
}

func NewTokenChan(delay int, num_tokens int, name string) *TokenChan {
	var itm TokenChan
	itm.open_tokens = make(map[string]int)
	itm.broadcast_chan = make(chan struct{})
	itm.name = name
	itm.max_cnt = num_tokens
	return &itm
}
func (tc *TokenChan) SetAutoFill() {
}

func (tc *TokenChan) qToken(basename string) bool {
	cnt, ok := tc.open_tokens[basename]
	if ok && (cnt < tc.max_cnt) {
		// If we haven't reached the limit yet
		return false
	}
	return ok
}
func (tc *TokenChan) loopToken(basename string) {
	// Stay in here until the entry does not exist
	for success := tc.qToken(basename); success; success = tc.qToken(basename) {
		tc.Unlock()
		<-tc.broadcast_chan
		tc.Lock()
	}
}

func (tc *TokenChan) GetToken(basename string) {
	//log.Printf("Token requested for \"%v\"\n", basename)
	tc.Lock()
	tc.loopToken(basename)
	// If we have got there then it doesn't exist in the map
	// So add it and grant the token
	tc.open_tokens[basename] = tc.open_tokens[basename] + 1
	tc.Unlock()
	//log.Printf("Token Granted for \"%v\"\n", basename)
}
func (tc *TokenChan) TryGetToken(basename string) bool {
	// This is a variant of GetToken that doesn't block
	// it returns true if it can give you a token
	// false otherwise
	//log.Printf("Token requested for \"%v\"\n", basename)
	tc.Lock()
	defer tc.Unlock()
	if tc.qToken(basename) {
		// The token is in use
		return false
	} else {
		// If we have got there then it doesn't exist in the map
		// So add it and grant the token
		tc.open_tokens[basename] = tc.open_tokens[basename] + 1
		return true
	}
	return true
}

func (tc *TokenChan) PutToken(basename string) {
	if basename == "" {
		return
	}
	//log.Printf("Token Returning for \"%v\", %s\n",basename, tc.name)

	tc.Lock()
	//log.Printf("Locked for \"%v\"\n",basename)
	cnt, ok := tc.open_tokens[basename]
	if ok {
		if cnt > 1 {
			cnt--
			tc.open_tokens[basename] = cnt
		} else {
			delete(tc.open_tokens, basename)
			// Close the current channel to broadcast to everyone listening on it
			close(tc.broadcast_chan)
			// Create a new one in its place
			tc.broadcast_chan = make(chan struct{})
		}
	} else {
		// Put on a token we don't have
		log.Fatalf("Attempted to put token that we don't have:\"%v\"", basename)
	}
	//log.Printf("Token Returned for \"%v\", %s\n",basename, tc.name)
	tc.Unlock()
}
func (tc *TokenChan) TryPutToken(basename string) {
	tc.PutToken(basename)
}
