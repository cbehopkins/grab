package grab

import (
	"fmt"
	"github.com/temoto/robotstxt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

type RobotCache struct {
	sync.Mutex
	cache    map[string]*robotstxt.RobotsData
	log_file *os.File
}

func NewRobotCache() *RobotCache {
	itm := new(RobotCache)
	itm.cache = make(map[string]*robotstxt.RobotsData)
	var err error
	itm.log_file, err = os.Create("/tmp/invalid_urls")
	check(err)
	return itm
}
func (rc *RobotCache) Close() {
	if rc.log_file != nil {
		rc.log_file.Close()
	}
}
func (rc *RobotCache) AllowUrl(urls Url) bool {
	rc.Lock()
	defer rc.Unlock()
	ur, err := url.Parse(urls.Url())
	if err != nil {
		fmt.Fprintf(rc.log_file, "%v\n", urls.Url())
	}
	bn := ur.Hostname()
	if bn != "" {
		fmt.Println("looking for robots.txt for ", bn, urls.Url())
		robots_loc := "http://" + bn + "/robots.txt"

		var robots *robotstxt.RobotsData
		var ok bool
		robots, ok = rc.cache[bn]
		if !ok {
			timeout := time.Duration(5 * time.Second)
			client := http.Client{
				Timeout: timeout,
			}

			resp, err := client.Get(robots_loc)
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
