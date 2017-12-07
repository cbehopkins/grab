// +build !race

package grab

import (
	"log"
	"testing"
	"time"
)

func TestCountBasic00(t *testing.T) {
  // Check that we don't lock if there is no work to do
	tc := NewOutCounter()
  tc.Add()
  tc.Dec()
	log.Println("waiting")
	tc.Wait()
	log.Println("Done")
}
func TestCountBasic0(t *testing.T) {
	tc := NewOutCounter()
	tc.Add()
	go func() {
		time.Sleep(100 * time.Millisecond)
		tc.Dec()
	}()
	log.Println("waiting")
	tc.Wait()
	log.Println("Done")
}
func TestCountBasic1(t *testing.T) {
	tc := NewOutCounter()
	tc.Add()
	go func() {
		tc.Dec()
	}()
	log.Println("waiting")
	tc.Wait()
	log.Println("Done")
}
func TestCountBasic2(t *testing.T) {
	tc := NewOutCounter()
	tc.Add()
	tc.Dec()
	tc.Add()
	go func() {
		time.Sleep(100 * time.Millisecond)
		tc.Dec()
	}()
	log.Println("waiting")
	tc.Wait()
	log.Println("Done")
}
