package grab

import (
	"fmt"
	"testing"
)

func TestRobot(t *testing.T) {
	test_urls := []string{
		"https://www.google.com/fred.html",
		"http://www.rufty.org.uk/index.html",
		"https://www.google.com/index.html?q=4",
		"https://www.google.com/groups/gonuts",
	}
	rc := NewRobotCache()
	for _, test_url := range test_urls {
		allowed := rc.AllowUrl(NewUrl(test_url))
		fmt.Printf("%v is %v\n", test_url, allowed)
	}
}
