package grab

import (
	"log"
	"time"
)

// StartTimer creates a way to detect operations that take too long
// Call start timer before the operation and it returns a channel
// Close this channel at the end of the operation
// You'll get a fatal error if this takes more than 40 seconds
func StartTimer(intIn ...interface{}) chan struct{} {
	var timeoutTimer *time.Timer
	var dbgMessage string
	for _, inIf := range intIn {
		switch inIf.(type) {
		case nil:
		case time.Duration:
			timeoutTimer = time.NewTimer(inIf.(time.Duration))
		case string:
			dbgMessage = dbgMessage + inIf.(string)
		default:
			log.Fatal("invalid type into StartTimer")
		}

	}
	if timeoutTimer == nil {
		timeoutTimer = time.NewTimer(time.Second * 40)
	}
	closer := make(chan struct{})
	go func() {
		select {
		case <-timeoutTimer.C:
			log.Fatal("Timed Out:", dbgMessage)
		case <-closer:
			timeoutTimer.Stop()
			return
		}
	}()
	return closer
}

func abortableSleep(t time.Duration, closeFunc func() bool) bool {
	startTime := time.Now()
	for {
		if closeFunc() {
			return true
		}
		if time.Since(startTime) > t {
			return false
		}
		time.Sleep(time.Second)
	}
}

func withTimeout(df func(), errorString string) {
	downloadComplete := make(chan struct{})

	go func() {
		df()
		close(downloadComplete)
	}()
	// Implement a timeout on the Fetch Work
	timeoutTimer := time.NewTimer(FetchTimeout + time.Second)
	select {
	case <-downloadComplete:
		timeoutTimer.Stop()
	case <-timeoutTimer.C:
		log.Println(errorString)
	}
	// Return the token at the end
}
