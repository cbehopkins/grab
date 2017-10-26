package grab

import (
	"fmt"
	"sync"
)

type uniStore struct {
	unvisit_urls *UrlMap
	visited_urls *UrlMap
	dv           *DomVisit
}

func newUniStore(dir string, compact bool, bad_url_fn string) *uniStore {
	itm := new(uniStore)
	itm.dv = NewDomVisit(bad_url_fn)
	visited_fname := dir + "/visited.gkvlite"
	unvisit_fname := dir + "/unvisit.gkvlite"

	// Open the maps and do not overwrite any we find
	itm.visited_urls = NewUrlMap(visited_fname, false, compact)
	itm.unvisit_urls = NewUrlMap(unvisit_fname, false, compact)

	itm.visited_urls.SetWriteCache()
	itm.visited_urls.SetReadCache()
	itm.unvisit_urls.SetWriteCache()
	itm.unvisit_urls.SetReadCache()
	return itm
}
func (usr uniStore) getDv() *DomVisit {
	return usr.dv
}
func (usr uniStore) Seed(url_fn string, promiscuous bool) chan Url {
	return usr.dv.Seed(url_fn, promiscuous)
}

func (usr uniStore) GoodUrl(url_in Url) bool {
	return usr.dv.GoodUrl(url_in)
}
func (usr uniStore) VisitedQ(url_in string) bool {
	return usr.dv.VisitedQ(url_in)
}

func (usr uniStore) VisitedA(url_in string) bool {
	return usr.dv.VisitedA(url_in)
}

func (usr uniStore) closeS() {
	usr.unvisit_urls.Close()
	usr.visited_urls.Close()
}
func (usr uniStore) closeDv() {
	usr.dv.Close()
}
func (usr uniStore) visitSize() int {
	return usr.visited_urls.Size()
}
func (usr uniStore) visitCount() int {
	return usr.visited_urls.Count()
}
func (usr uniStore) unvisitCount() int {
	return usr.unvisit_urls.Count()
}
func (usr uniStore) unvisitSize() int {
	return usr.unvisit_urls.Size()
}
func (usr uniStore) waitLoad() {
	usr.dv.WaitLoad()
}
func (usr uniStore) flushSync() {
	usr.unvisit_urls.Flush()
	usr.visited_urls.Flush()
	usr.unvisit_urls.Sync()
	usr.visited_urls.Sync()
}
func (usr uniStore) Visit() chan Url {
	return usr.unvisit_urls.Visit()
}
func (usr uniStore) VisitAll(closeChan chan struct{}) chan Url {
	return usr.unvisit_urls.VisitAll(closeChan)
}
func (usr uniStore) getMissing(refr *TokenChan) map[string]struct{} {
	return usr.unvisit_urls.VisitMissing(refr)
}

func (usr uniStore) setVisited(urs Url) {
	//fmt.Println("**********Setting")
	usr.visited_urls.Set(urs)
	//fmt.Println("**********Delete Test")
	if usr.unvisit_urls != nil {
		//fmt.Println("Calling Delete")
		usr.unvisit_urls.Delete(urs)
	}
	//fmt.Println("***********Set Complete")
}

// runChan will run through the inputs you give it
// and store it into unvisted (unless it already has been visited)
// Some sophistication needed to make sure it was the correct type of visit
func (usr uniStore) runChan(input_chan <-chan Url, dbg_name string) *sync.WaitGroup {
	//usr.visited_urls.LockTest()

	var wg sync.WaitGroup
	wg.Add(1)
	go usr.runChanW(input_chan, dbg_name, &wg)
	return &wg
}
func (usr uniStore) retrieveUnvisted(urv string) *Url {
	if usr.visited_urls.ExistS(urv) {
		usr.unvisit_urls.DeleteS(urv)
		return nil
	} else {
		// Get the url structure off disk
		new_url := usr.unvisit_urls.GetUrl(urv)
		//_ = new_url.Base()
		return &new_url
	}

}

// The worker funciton for runChan
func (usr uniStore) runChanW(input_chan <-chan Url, dbg_name string, wg *sync.WaitGroup) {
	for urv := range input_chan {
		if dbg_name != "" {
			fmt.Printf("runChan, %s RX:%v\n", dbg_name, urv)
		}
		promiscuous, shallow, ok := usr.visited_urls.Properties(urv)
		new_shallow := urv.GetShallow() || urv.GetPromiscuous()
		if ok && (!new_shallow || shallow || promiscuous) {
			// If we've already visited it then nothing to do
			if dbg_name != "" {
				fmt.Printf("We've already visited %s\n", urv)
			}
		} else {
			// If we previously visited this url
			// but at the time we didn't fully grab it
			if new_shallow && !shallow {
				usr.visited_urls.Delete(urv)
			}
			// If we haven't visited it, then add it to the list of places to visit
			if dbg_name != "" {
				fmt.Println("Adding as unvisited url:", urv)
			}
			usr.unvisit_urls.Set(urv)
		}
		if dbg_name != "" {
			fmt.Printf("runChan, %s, has been set\n", dbg_name)
		}
	}
	wg.Done()
}
func (usr *uniStore) lockTest() {
	usr.visited_urls.LockTest()
	usr.unvisit_urls.LockTest()
}
func (usr *uniStore) dumpVisited(filename string) {
	the_chan := usr.visited_urls.VisitAll(nil)
	SaveFile(filename, the_chan, nil)
}
func (usr *uniStore) dumpUnvisited(filename string) {
	the_chan := usr.unvisit_urls.VisitAll(nil)
	SaveFile(filename, the_chan, nil)
}
func (usr *uniStore) clearVisited() {
	list := make([]Url, 0, 10000)
	s := Spinner{}
	cnt := 0
	length := usr.visited_urls.Count()
	fmt.Println("Resetting Unvisited", length)
	for usr.visited_urls.Size() > 0 {
		cnt_backup := cnt
		visited_chan := usr.visited_urls.Visit()
		for v := range visited_chan {
			list = append(list, v)
			s.PrintSpin(cnt)
			cnt++
		}
		cnt = cnt_backup
		for _, v := range list {
			usr.visited_urls.Delete(v)
			usr.unvisit_urls.Set(v)
			s.PrintSpin(cnt)
			cnt++
		}

		// reset list to 0 length - but retain capacity
		list = list[:0]
	}
}
