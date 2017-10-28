package grab

import (
	"log"
	"testing"
	"time"
)

func TestWorkToken0(t *testing.T) {
	wt := newWkTok(10)
	wt.getTok()
	log.Println("Got")
	wt.putTok()
	log.Println("Put")
	log.Println("Waiting")
	wt.wait()

}
func TestWorkToken1(t *testing.T) {
	wt := newWkTok(10)
	wt.getTok()
	wt.putTok()
	log.Println("Waiting")
	wt.wait()
}

func TestWorkToken2(t *testing.T) {
	wt := newWkTok(10)
	wt.getTok()
	go func() {
		<-time.After(1 * time.Second)
		wt.putTok()
	}()
	log.Println("Waiting")
	wt.wait()
}
func TestWorkToken3(t *testing.T) {
	wt := newWkTok(2)
	wt.getTok()
	log.Println("Got")
	wt.getTok()
	log.Println("Got")
	wt.putTok()
	log.Println("Put")
	wt.getTok()
	log.Println("Got")
	go func() {
		<-time.After(1 * time.Second)
		wt.putTok()
		log.Println("Put")
	}()
	go func() {
		<-time.After(2 * time.Second)
		wt.putTok()
		log.Println("Put")
	}()
	log.Println("Waiting")
	wt.wait()
}
func TestWorkToken4(t *testing.T) {
	wt := newWkTok(2)
	wt.getTok()
	log.Println("Got")
	wt.getTok()
	log.Println("Got")
	go func() {
		<-time.After(1 * time.Second)
		wt.putTok()
		log.Println("Put")
	}()
	go func() {
		<-time.After(2 * time.Second)
		wt.putTok()
		log.Println("Put")
	}()
	wt.getTok()
	log.Println("Got")
	wt.putTok()
	log.Println("Put")
	log.Println("Waiting")
	wt.wait()
}
