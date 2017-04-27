package grab

import "fmt"

type Spinner struct {
	status     int
	scaler     int
	scaled_inc int
}

var spinn_array = []string{"|", "/", "-", "\\"}

func (s Spinner) translateSpinner() string {
	return spinn_array[s.status]
}

func (s *Spinner) PrintSpin(cnt int) {
	if s.scaler != 0 {
		s.scaled_inc++
		if s.scaled_inc >= s.scaler {
			s.scaled_inc = 0
		}
	}
	if s.scaled_inc == 0 {
		fmt.Printf("%s %8d\b\b\b\b\b\b\b\b\b\b", s.translateSpinner(), cnt)
		s.status++
		if s.status >= len(spinn_array) {
			s.status = 0
		}
	}

}
