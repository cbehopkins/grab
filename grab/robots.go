package grab

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/temoto/robotstxt"
)

// RobotCache forms a cache structure on robots.tst
// rather than fetching a robots'txt for every fetch
// we can be efficient and fetch once and check against that
// it's not flawless, but for these purposes I prefer efficiency over accuracy
type RobotCache struct {
	sync.Mutex
	cache   map[string]*robotstxt.RobotsData
	logFile *os.File
}

// NewRobotCache - Create a new Robots cache structure
func NewRobotCache() *RobotCache {
	itm := new(RobotCache)
	itm.cache = make(map[string]*robotstxt.RobotsData)
	var err error
	itm.logFile, err = os.Create(os.TempDir() + "/invalid_urls")
	check(err)
	return itm
}

// Close down the Cache
func (rc *RobotCache) Close() {
	if rc.logFile != nil {
		rc.logFile.Close()
	}
}

// AllowURL returns true if we are allowed to fetch this URL
func (rc *RobotCache) AllowURL(urls URL) bool {
	rc.Lock()
	defer rc.Unlock()
	ur, err := url.Parse(urls.URL())
	if err != nil {
		fmt.Fprintf(rc.logFile, "%v\n", urls.URL())
	}
	bn := ur.Hostname()
	if bn != "" {
		fmt.Println("looking for robots.txt for ", bn, urls.URL())
		robotsLoc := "http://" + bn + "/robots.txt"

		var robots *robotstxt.RobotsData
		var ok bool
		robots, ok = rc.cache[bn]
		if !ok {
			timeout := time.Duration(GrabTimeout)
			client := http.Client{
				Timeout: timeout,
			}

			resp, err := client.Get(robotsLoc)
			if err != nil {
				return true
			}
			robots, err = robotstxt.FromResponse(resp)
			resp.Body.Close()

			if err != nil {
				//log.Println("Error parsing robots.txt:", err.Error())
				return true
			}
			rc.cache[bn] = robots
		}

		path := ur.EscapedPath()
		allow := robots.TestAgent(path, "FooBot")
		return allow
	}
	return true
}
