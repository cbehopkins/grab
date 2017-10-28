package grab

import (
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestTok0(t *testing.T) {
	tks := NewTokenChan(0, "")
	tks.GetToken("fred")
	log.Println("Got a token for fred")
	tks.PutToken("fred")
	log.Println("Returned Fred")

	tks.GetToken("fred")
	log.Println("Got a token for fred")
	tks.PutToken("fred")
	log.Println("Returned Fred")
}

func TestTok1(t *testing.T) {
	tks := NewTokenChan(0, "")
	tks.GetToken("fred")
	log.Println("Got a token for fred")
	tks.GetToken("bob")
	log.Println("Got a token for bob")

	tks.PutToken("fred")
	log.Println("Returned Fred")
	tks.PutToken("bob")
	log.Println("Returned Bob")

	tks.GetToken("fred")
	log.Println("Got a token for fred")
	tks.PutToken("fred")
	log.Println("Returned Fred")
}
func TestTok2(t *testing.T) {
	tks := NewTokenChan(0, "")
	tks.GetToken("fred")
	log.Println("Got a token for fred")
	tks.GetToken("bob")
	log.Println("Got a token for bob")
	tks.PutToken("bob")
	log.Println("Returned Bob")
	tks.PutToken("fred")
	log.Println("Returned Fred")

	tks.GetToken("fred")
	log.Println("Got a token for fred")
	tks.PutToken("fred")
	log.Println("Returned Fred")
}
func TestTok3(t *testing.T) {
	var wg sync.WaitGroup

	tks := NewTokenChan(0, "")
	tks.GetToken("fred")
	log.Println("Got a token for fred")
	tks.GetToken("bob")
	log.Println("Got a token for bob")

	wg.Add(1)
	go func() {
		tks.GetToken("bob")
		log.Println("Got another token for bob")
		tks.PutToken("bob")
		log.Println("Returned Bob")
		wg.Done()
	}()

	tks.PutToken("fred")
	log.Println("Returned Fred")
	tks.PutToken("bob")
	log.Println("Returned Bob")

	tks.GetToken("fred")
	log.Println("Got a token for fred")
	tks.PutToken("fred")
	log.Println("Returned Fred")
	wg.Wait()
}
func TestTok4(t *testing.T) {
	var wg sync.WaitGroup

	tks := NewTokenChan(0, "")
	tks.GetToken("fred")
	log.Println("Got a token for fred")
	tks.GetToken("bob")
	log.Println("Got a token for bob")

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			tks.GetToken("bob")
			log.Println("Got another token for bob")
			tks.PutToken("bob")
			log.Println("Returned Bob")
			wg.Done()
		}()
	}
	tks.PutToken("fred")
	log.Println("Returned Fred")
	tks.PutToken("bob")
	log.Println("Returned Bob")
	wg.Wait()

	tks.GetToken("fred")
	log.Println("Got a token for fred")
	tks.PutToken("fred")
	log.Println("Returned Fred")
}
func TestTok5(t *testing.T) {
	var wg sync.WaitGroup

	tks := NewTokenChan(0, "")
	tks.GetToken("fred")
	log.Println("Got a token for fred")
	tks.GetToken("bob")
	log.Println("Got a token for bob")
	numLoops := 100
	wg.Add(numLoops)

	for i := 0; i < numLoops; i++ {
		go func() {
			tks.GetToken("bob")
			log.Println("Got another token for bob")
			yourTime := rand.Int31n(1000)
			time.Sleep(time.Duration(yourTime) * time.Millisecond)
			tks.PutToken("bob")
			log.Println("Returned Bob")
			wg.Done()
		}()
	}
	tks.PutToken("fred")
	log.Println("Returned Fred")
	tks.PutToken("bob")
	log.Println("Returned Bob")
	wg.Wait()

	tks.GetToken("fred")
	log.Println("Got a token for fred")
	tks.PutToken("fred")
	log.Println("Returned Fred")
}
func TestTok6(t *testing.T) {
	var wg sync.WaitGroup
	domains := []string{"bob", "fred", "steve", "wibble"}
	locks := make([]int, len(domains))

	tks := NewTokenChan(0, "")
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			randTok := rand.Intn(4)
			tokenAct := domains[randTok]
			tks.GetToken(tokenAct)
			//log.Println("Got a token for ", token_act)
			if locks[randTok] == 0 {
				locks[randTok] = 1
			} else {
				log.Fatal("Bugger, We've been given the lock for someone else's data")
			}
			yourTime := rand.Int31n(1000)
			time.Sleep(time.Duration(yourTime) * time.Millisecond)

			if locks[randTok] == 1 {
				locks[randTok] = 0
			} else {
				log.Fatal("Bugger, someone else messed with it while we had the lock")
			}

			tks.PutToken(tokenAct)
			//log.Println("Returned ", token_act)
			wg.Done()
		}()
	}
	wg.Wait()
}
func TestTok7(t *testing.T) {
	var wg sync.WaitGroup
	domains := []string{"bob", "fred", "steve", "wibble"}
	locks := make([]int, 4)

	maxProcs := 2
	tks := NewTokenChan(maxProcs, "")
	for i := 0; i < 100; i++ {
		wg.Add(1)
		var masterLock sync.Mutex
		go func() {
			randTok := rand.Intn(4)
			tokenAct := domains[randTok]
			tks.GetToken(tokenAct)
			log.Println("Got another token for ", tokenAct)
			masterLock.Lock()
			if locks[randTok] < maxProcs {
				locks[randTok]++
			} else {
				log.Fatalf("Domain:%s,%v\n", tokenAct, locks)
			}
			masterLock.Unlock()
			yourTime := rand.Int31n(1000)
			time.Sleep(time.Duration(yourTime) * time.Millisecond)

			masterLock.Lock()
			if locks[randTok] <= maxProcs {
				locks[randTok]--
			} else if locks[randTok] == 0 {
				log.Fatal("Decrement of zero")
			} else {
				log.Fatalf("Release Domain:%s,%v\n", tokenAct, locks)
			}
			masterLock.Unlock()

			tks.PutToken(tokenAct)
			log.Println("Returned ", tokenAct)
			wg.Done()
		}()
	}
	wg.Wait()
}
