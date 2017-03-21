package grab

import (
	"log"
	"testing"
)

func TestDiskStore0(t *testing.T) {

	dkst := NewDkStore("/tmp/test.gkvlite")
	dkst.SetAny(23, "Hello")
	dkst.SetAny(25, "Goodbye")
	dkst.Flush()
	log.Println("We say:", dkst.GetString(23))
	log.Println("They say:", dkst.GetString(25))

	key_chan := dkst.GetIntKeys()
	for key := range key_chan {
		log.Println("The collection contains:", key)
	}
	dkst.Close()
}


func TestDiskStore1(t *testing.T) {

        dkst := NewDkStore("/tmp/test.gkvlite")
        dkst.SetAny("Hello1",23)
        dkst.SetAny("Goodbye1",25)
        dkst.Flush()
        log.Println("We say:", dkst.GetInt("Hello1"))
        log.Println("They say:", dkst.GetInt("Goodbye1"))

        key_chan := dkst.GetAnyKeys()
        for key := range key_chan {
                log.Println("The collection contains:", string(key))
        }
	dkst.Close()
}

func TestDiskStore2(t *testing.T) {

        dkst := NewDkStore("/tmp/test.gkvlite")
	defer dkst.Close()	

	// Let's pretend to have some urls
	var url_0 Url
	var url_1 Url
	var url_2 Url
	url_0 = "http://here.com"
	url_1 = "http://there.com"	
	url_2 = "http://nowhere.com"
	if dkst.Size() >0 {
		log.Fatal("Bigger than zero")
	}
	dkst.SetAny(url_0, "")
	dkst.SetAny(url_1, "")
	if dkst.Size() ==0 {
		log.Fatal("Size is zero")
	}


	if !dkst.Exist(url_0) {
		log.Fatal("0 does not exist")
	}
	if !dkst.Exist(url_1) {
		log.Fatal("1 does not exist")
	}
	if dkst.Exist(url_2) {
		log.Fatal("2 does exist")
	}
	dkst.Flush()
	key_chan := dkst.GetAnyKeys()
        for key := range key_chan {
                log.Println("The collection contains:", string(key))
        }

	// Delete returns true if suceeds
	_ = dkst.Delete(url_0)
	_ = dkst.Delete(url_1)

	if dkst.Size() >0 {
                log.Fatal("Bigger than zero after delete")
        }


}
