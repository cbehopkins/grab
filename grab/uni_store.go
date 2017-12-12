package grab

import (
	"fmt"
	"sync"
)

// uniStore is a universal store collecting the vivisted and unvisited structures
type uniStore struct {
	unvisitUrls *URLMap
	visitedUrls *URLMap
	dv          *DomVisit
}

func newUniStore(dir string, compact bool, badURLFn string) *uniStore {
	itm := new(uniStore)
	itm.dv = NewDomVisit(badURLFn)
	visitedFname := dir + "/visited.gkvlite"
	unvisitFname := dir + "/unvisit.gkvlite"

	// Open the maps and do not overwrite any we find
	itm.visitedUrls = NewURLMap(visitedFname, false, compact)
	itm.unvisitUrls = NewURLMap(unvisitFname, false, compact)

	itm.visitedUrls.SetWriteCache()
	itm.visitedUrls.SetReadCache()
	itm.unvisitUrls.SetWriteCache()
	itm.unvisitUrls.SetReadCache()
	return itm
}
func (usr uniStore) getDv() *DomVisit {
	return usr.dv
}

// Seed exported
func (usr uniStore) Seed(urlFn string, promiscuous bool) chan URL {
	return usr.dv.Seed(urlFn, promiscuous)
}

// GoodURL exported
func (usr uniStore) GoodURL(urlIn URL) bool {
	return usr.dv.GoodURL(urlIn)
}

// VisitedQ exported
func (usr uniStore) VisitedQ(urlIn string) bool {
	return usr.dv.VisitedQ(urlIn)
}

// VisitedA exported
func (usr uniStore) VisitedA(urlIn string) bool {
	return usr.dv.VisitedA(urlIn)
}

func (usr uniStore) closeS() {
	usr.unvisitUrls.Close()
	usr.visitedUrls.Close()
}
func (usr uniStore) closeDv() {
	usr.dv.Close()
}
func (usr uniStore) visitSize() int {
	return usr.visitedUrls.Size()
}
func (usr uniStore) visitCount() int {
	return usr.visitedUrls.Count()
}
func (usr uniStore) unvisitCount() int {
	return usr.unvisitUrls.Count()
}
func (usr uniStore) unvisitSize() int {
	return usr.unvisitUrls.Size()
}
func (usr uniStore) waitLoad() {
	usr.dv.WaitLoad()
}
func (usr uniStore) flushSync() {
	usr.unvisitUrls.Flush()
	usr.visitedUrls.Flush()
	usr.unvisitUrls.Sync()
	usr.visitedUrls.Sync()
}
func (usr uniStore) Visit() chan URL {
	return usr.unvisitUrls.Visit()
}
func (usr uniStore) VisitAll(closeChan chan struct{}) chan URL {
	return usr.unvisitUrls.VisitAll(closeChan)
}
func (usr uniStore) VisitFrom(startURL URL) chan URL {
	return usr.unvisitUrls.VisitFrom(startURL)
}
func (usr uniStore) VisitFromBatch(startURL URL) chan []URL {
	return usr.unvisitUrls.VisitFromBatch(startURL)
}
func (usr uniStore) getMissing(refr *TokenChan) map[string]struct{} {
	return usr.unvisitUrls.VisitMissing(refr)
}

func (usr uniStore) setVisited(urs URL) {
	//fmt.Println("**********Setting")
	usr.visitedUrls.Set(urs)
	//fmt.Println("**********Delete Test")
	if usr.unvisitUrls != nil {
		//fmt.Println("Calling Delete")
		usr.unvisitUrls.Delete(urs)
	}
	//fmt.Println("***********Set Complete")
}

// runChan will run through the inputs you give it
// and store it into unvisted (unless it already has been visited)
// Some sophistication needed to make sure it was the correct type of visit
func (usr uniStore) runChan(inputChan <-chan URL, dbgName string) *sync.WaitGroup {
	//usr.visited_urls.LockTest()

	var wg sync.WaitGroup
	wg.Add(1)
	go usr.runChanW(inputChan, dbgName, &wg)
	return &wg
}
func (usr uniStore) retrieveUnvisted(urv string) *URL {
	if usr.visitedUrls.ExistS(urv) {
		usr.unvisitUrls.DeleteS(urv)
		return nil
	}
	// Get the url structure off disk
	newURL := usr.unvisitUrls.GetURL(urv)
	//_ = new_url.Base()
	return &newURL
}

// The worker funciton for runChan
func (usr uniStore) runChanW(inputChan <-chan URL, dbgName string, wg *sync.WaitGroup) {
	for urv := range inputChan {
		if dbgName != "" {
			fmt.Printf("runChan, %s RX:%v\n", dbgName, urv)
		}
		promiscuous, shallow, ok := usr.visitedUrls.Properties(urv)
		newShallow := urv.GetShallow() || urv.GetPromiscuous()
		if ok && (!newShallow || shallow || promiscuous) {
			// If we've already visited it then nothing to do
			if dbgName != "" {
				fmt.Printf("We've already visited %s\n", urv)
			}
		} else {
			// If we previously visited this url
			// but at the time we didn't fully grab it
			if newShallow && !shallow {
				usr.visitedUrls.Delete(urv)
			}
			// If we haven't visited it, then add it to the list of places to visit
			if dbgName != "" {
				fmt.Println("Adding as unvisited url:", urv)
			}
			usr.unvisitUrls.Set(urv)
		}
		if dbgName != "" {
			fmt.Printf("runChan, %s, has been set\n", dbgName)
		}
	}
	wg.Done()
}
func (usr *uniStore) lockTest() {
	usr.visitedUrls.LockTest()
	usr.unvisitUrls.LockTest()
}
func (usr *uniStore) dumpVisited(filename string) {
	theChan := usr.visitedUrls.VisitAll(nil)
	SaveFile(filename, theChan, nil)
}
func (usr *uniStore) dumpUnvisited(filename string) {
	theChan := usr.unvisitUrls.VisitAll(nil)
	SaveFile(filename, theChan, nil)
}
func (usr *uniStore) clearVisited() {
	list := make([]URL, 0, 10000)
	s := Spinner{}
	cnt := 0
	length := usr.visitedUrls.Count()
	fmt.Println("Resetting Unvisited", length)
	for usr.visitedUrls.Size() > 0 {
		cntBackup := cnt
		visitedChan := usr.visitedUrls.Visit()
		for v := range visitedChan {
			list = append(list, v)
			s.PrintSpin(cnt)
			cnt++
		}
		cnt = cntBackup
		for _, v := range list {
			usr.visitedUrls.Delete(v)
			usr.unvisitUrls.Set(v)
			s.PrintSpin(cnt)
			cnt++
		}

		// reset list to 0 length - but retain capacity
		list = list[:0]
	}
}
