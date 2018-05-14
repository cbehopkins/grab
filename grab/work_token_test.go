package grab

import (
	"log"
	"testing"
	"time"
)

func TestWorkToken0(t *testing.T) {
	wt := NewWkTok(10)
	wt.GetTok()
	log.Println("Got")
	wt.PutTok()
	log.Println("Put")
	log.Println("Waiting")
	wt.Wait()

}
func TestWorkToken1(t *testing.T) {
	wt := NewWkTok(10)
	wt.GetTok()
	wt.PutTok()
	log.Println("Waiting")
	wt.Wait()
}

func TestWorkToken2(t *testing.T) {
	wt := NewWkTok(10)
	wt.GetTok()
	go func() {
		<-time.After(1 * time.Second)
		wt.PutTok()
	}()
	log.Println("Waiting")
	wt.Wait()
}
func TestWorkToken3(t *testing.T) {
	wt := NewWkTok(2)
	wt.GetTok()
	log.Println("Got")
	wt.GetTok()
	log.Println("Got")
	wt.PutTok()
	log.Println("Put")
	wt.GetTok()
	log.Println("Got")
	go func() {
		<-time.After(1 * time.Second)
		wt.PutTok()
		log.Println("Put")
	}()
	go func() {
		<-time.After(2 * time.Second)
		wt.PutTok()
		log.Println("Put")
	}()
	log.Println("Waiting")
	wt.Wait()
}
func TestWorkToken4(t *testing.T) {
	wt := NewWkTok(2)
	wt.GetTok()
	log.Println("Got")
	wt.GetTok()
	log.Println("Got")
	go func() {
		<-time.After(1 * time.Second)
		wt.PutTok()
		log.Println("Put")
	}()
	go func() {
		<-time.After(2 * time.Second)
		wt.PutTok()
		log.Println("Put")
	}()
	wt.GetTok()
	log.Println("Got")
	wt.PutTok()
	log.Println("Put")
	log.Println("Waiting")
	wt.Wait()
}
