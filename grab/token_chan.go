package grab

import (
	"log"
	"sync"
)

// ToC is the channel token
type ToC chan struct{}

// TokenChan is used to check that we have a limited number of things fetching at once
// It's convenitent to use because we can tret things like a channel read
type TokenChan struct {
	sync.Mutex
	openTokens    map[string]int
	broadcastChan chan struct{}
	name          string
	maxCnt        int
}

// NewTokenChan creates a new token chan tracker
func NewTokenChan(numTokens int, name string) *TokenChan {
	var itm TokenChan
	itm.openTokens = make(map[string]int)
	itm.broadcastChan = make(chan struct{})
	itm.name = name
	itm.maxCnt = numTokens
	return &itm
}

func (tc *TokenChan) qToken(basename string) bool {
	cnt, ok := tc.openTokens[basename]
	// Return true until there is a token available
	//log.Printf("Token %s, cnt %d, ok %v",basename, cnt,ok)
	if ok && (cnt >= tc.maxCnt) {
		// If we have reached the limit
		return true
	}
	return ok
}
func (tc *TokenChan) loopToken(basename string) {
	// Stay in here until the entry does not exist
	for success := tc.qToken(basename); success; success = tc.qToken(basename) {
		chanToListen := tc.broadcastChan
		tc.Unlock()
		<-chanToListen
		tc.Lock()
	}
}

// GetToken get a token for the supplied basename
// blocks until it suceeds
func (tc *TokenChan) GetToken(basename string) {
	if basename == "" {
		return
	}
	//log.Printf("Token requested for \"%v\"\n", basename)
	tc.Lock()
	tc.loopToken(basename)
	// If we have got there then it doesn't exist in the map
	// So add it and grant the token
	tc.openTokens[basename] = tc.openTokens[basename] + 1
	tc.Unlock()
	//log.Printf("Token Granted for \"%v\"\n", basename)
}

// TryGetToken attempts to get a token
// return true if it got a token
// returns false if it fails
// does not block
func (tc *TokenChan) TryGetToken(basename string) bool {
	if basename == "" {
		return true
	}
	// This is a variant of GetToken that doesn't block
	// it returns true if it can give you a token
	// false otherwise
	//log.Printf("Token requested for \"%v\"\n", basename)
	tc.Lock()
	defer tc.Unlock()
	if tc.qToken(basename) {
		// The token is in use
		return false
	}
	// If we have got there then it doesn't exist in the map
	// So add it and grant the token
	tc.openTokens[basename] = tc.openTokens[basename] + 1
	return true
}

// PutToken returns the token after use
func (tc *TokenChan) PutToken(basename string) {
	if basename == "" {
		return
	}
	//log.Printf("Token Returning for \"%v\", %s\n",basename, tc.name)

	tc.Lock()
	//log.Printf("Locked for \"%v\"\n",basename)
	cnt, ok := tc.openTokens[basename]
	if ok {
		if cnt > 1 {
			cnt--
			tc.openTokens[basename] = cnt
		} else {
			delete(tc.openTokens, basename)
			// Close the current channel to broadcast to everyone listening on it
			close(tc.broadcastChan)
			// Create a new one in its place
			tc.broadcastChan = make(chan struct{})
		}
	} else {
		// Put on a token we don't have
		log.Fatalf("Attempted to put token that we don't have:\"%v\"", basename)
	}
	//log.Printf("Token Returned for \"%v\", %s\n",basename, tc.name)
	tc.Unlock()
}

// TryPutToken alias to allow consustent naming
func (tc *TokenChan) TryPutToken(basename string) {
	tc.PutToken(basename)
}

// BaseExist - Check if the basename exists
func (tc *TokenChan) BaseExist(urx string) bool {
	tc.Lock()
	_, ok := tc.openTokens[urx]
	tc.Unlock()
	return ok
}
