package grab

import (
	"log"
	"math/rand"
	"os"
	"testing"
)

func URLSrc(cnt int) (chan URL, map[string]struct{}) {
	theChan := make(chan URL)
	theMap := make(map[string]struct{})
	maxStrLen := 300
	go func() {
		for i := 0; i < cnt; i++ {

			strLen := rand.Int31n(int32(maxStrLen)) + 1
			tstString := RandStringBytesMaskImprSrc(int(strLen))
			theMap[tstString] = struct{}{}
			tmpURL := NewURL(tstString)

			theChan <- tmpURL
		}
		close(theChan)
	}()
	return theChan, theMap
}

func TestGobWrite0(t *testing.T) {
	outCount := NewOutCounter()
	outCount.Add()

	filename := "gob_test.txt"
	theChan, theMap := URLSrc(100000)

	go SaveGob(filename, theChan, outCount)
	defer os.Remove(filename)
	outCount.Wait()
	log.Println("Write Success!")

	theChan = make(chan URL)

	go LoadGob(filename, theChan, outCount, true)

	foundItems := make(map[string]struct{})
	for v := range theChan {
		urlf := v.URL()
		foundItems[urlf] = struct{}{}
		_, ok := theMap[urlf]

		if !ok {
			log.Fatal("Missing Key", v.URL())
		}
	}

	if len(foundItems) != len(theMap) {
		log.Fatal("Incorrect length", len(foundItems), len(theMap))
	}
}
