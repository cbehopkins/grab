package grab

import "fmt"

// Spinner is a little spinning progress widget
type Spinner struct {
	status    int
	scaler    int
	scaledInc int
}

var spinnArray = []string{"|", "/", "-", "\\"}

func (s Spinner) translateSpinner() string {
	return spinnArray[s.status]
}

// PrintSpin Call this with the current cnt
// each call will advance the spinner
// the count will be printed to screen
func (s *Spinner) PrintSpin(cnt int) {
	if s.scaler != 0 {
		s.scaledInc++
		if s.scaledInc >= s.scaler {
			s.scaledInc = 0
		}
	}
	if s.scaledInc == 0 {
		fmt.Printf("%s %8d\b\b\b\b\b\b\b\b\b\b", s.translateSpinner(), cnt)
		s.status++
		if s.status >= len(spinnArray) {
			s.status = 0
		}
	}

}
