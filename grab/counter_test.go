package grab

import (
        "log"
 	"time"
        "testing"
)

func TestCountBasic0(t *testing.T) {
	var tc OutCounter
	tc.Add()
 	go func () {
		time.Sleep(100 * time.Millisecond)
		tc.Dec()
	} ()
 	log.Println("waiting")
	tc.Wait()
 	log.Println("Done")
}
func TestCountBasic1(t *testing.T) {
	var tc OutCounter
	tc.Add()
 	go func () {
		tc.Dec()
	} ()
 	log.Println("waiting")
	tc.Wait()
 	log.Println("Done")
}
func TestCountBasic2(t *testing.T) {
	var tc OutCounter
	tc.Add()
	tc.Dec()
 	tc.Add()
	go func () {
		time.Sleep(100 * time.Millisecond)
		tc.Dec()
	} ()
 	log.Println("waiting")
	tc.Wait()
 	log.Println("Done")
}



