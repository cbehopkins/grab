package grab

import (
	"log"
	"math/rand"
	"testing"
)

func UrlSrc(cnt int) (chan Url, map[string]struct{}) {
	the_chan := make(chan Url)
	the_map := make(map[string]struct{})
	max_str_len := 300
	go func() {
		for i := 0; i < cnt; i++ {

			str_len := rand.Int31n(int32(max_str_len)) + 1
			tst_string := RandStringBytesMaskImprSrc(int(str_len))
			the_map[tst_string] = struct{}{}
			tmp_url := NewUrl(tst_string)

			the_chan <- tmp_url
		}
		close(the_chan)
	}()
	return the_chan, the_map
}

func TestGobWrite0(t *testing.T) {
	out_count := NewOutCounter()
	out_count.Add()

	filename := "gob_test.txt"
	the_chan, the_map := UrlSrc(100000)

	go SaveGob(filename, the_chan, out_count)
	out_count.Wait()
	log.Println("Write Success!")

	the_chan = make(chan Url)

	go LoadGob(filename, the_chan, out_count, true, false)

	found_items := make(map[string]struct{})
	for v := range the_chan {
		urlf := v.Url()
		found_items[urlf] = struct{}{}
		_, ok := the_map[urlf]

		if !ok {
			log.Fatal("Missing Key", v.Url())
		}
	}

	if len(found_items) != len(the_map) {
		log.Fatal("Incorrect length", len(found_items), len(the_map))
	}
}
