package grab

import (
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestTok0(t *testing.T) {
	tks := NewTokenChan(0, 0, "")
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
	tks := NewTokenChan(0, 0, "")
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
	tks := NewTokenChan(0, 0, "")
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

	tks := NewTokenChan(0, 0, "")
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

	tks := NewTokenChan(0, 0, "")
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

	tks := NewTokenChan(0, 0, "")
	tks.GetToken("fred")
	log.Println("Got a token for fred")
	tks.GetToken("bob")
	log.Println("Got a token for bob")

	for i := 0; i < 100; i++ {
		wg.Add(1)
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

	tks := NewTokenChan(0, 0, "")
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			rand_tok := rand.Intn(4)
			token_act := domains[rand_tok]
			tks.GetToken(token_act)
			//log.Println("Got a token for ", token_act)
			if locks[rand_tok] == 0 {
				locks[rand_tok] = 1
			} else {
				log.Fatal("Bugger, We've been given the lock for someone else's data")
			}
			yourTime := rand.Int31n(1000)
			time.Sleep(time.Duration(yourTime) * time.Millisecond)

			if locks[rand_tok] == 1 {
				locks[rand_tok] = 0
			} else {
				log.Fatal("Bugger, someone else messed with it while we had the lock")
			}

			tks.PutToken(token_act)
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

	max_procs := 2
	tks := NewTokenChan(0, max_procs, "")
	for i := 0; i < 100; i++ {
		wg.Add(1)
		var master_lock sync.Mutex
		go func() {
			rand_tok := rand.Intn(4)
			token_act := domains[rand_tok]
			tks.GetToken(token_act)
			log.Println("Got another token for ", token_act)
			master_lock.Lock()
			if locks[rand_tok] < max_procs {
				locks[rand_tok]++
			} else {
				log.Fatalf("Domain:%s,%v\n", token_act, locks)
			}
			master_lock.Unlock()
			yourTime := rand.Int31n(1000)
			time.Sleep(time.Duration(yourTime) * time.Millisecond)

			master_lock.Lock()
			if locks[rand_tok] <= max_procs {
				locks[rand_tok]--
			} else if locks[rand_tok] == 0 {
				log.Fatal("Decrement of zero")
			} else {
				log.Fatalf("Release Domain:%s,%v\n", token_act, locks)
			}
			master_lock.Unlock()

			tks.PutToken(token_act)
			log.Println("Returned ", token_act)
			wg.Done()
		}()
	}
	wg.Wait()
}
