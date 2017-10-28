package grab

import (
	"fmt"
	"testing"
)

func TestRobot(t *testing.T) {
	testURLs := []string{
		"https://www.google.com/fred.html",
		"http://www.rufty.org.uk/index.html",
		"https://www.google.com/index.html?q=4",
		"https://www.google.com/groups/gonuts",
	}
	rc := NewRobotCache()
	for _, testURL := range testURLs {
		allowed := rc.AllowURL(NewURL(testURL))
		fmt.Printf("%v is %v\n", testURL, allowed)
	}
}
