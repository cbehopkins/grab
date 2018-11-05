package grab

import (
	"image/jpeg"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"syscall"
	//"strings"
	"time"

	"github.com/cbehopkins/medorg"
)

const (
	// FetchTimeout - set timeout for fetchs
	FetchTimeout = 30 * time.Minute
	// GrabTimeout - Set timeut for grabs
	GrabTimeout = 5 * time.Second
	// GenChecksums - turns on generating checksums on files as we write them
	GenChecksums = true
)

// Test the jpeg for validity

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

// Block of constants around file permissions
const (
	OsRead       = 04
	OsWrite      = 02
	OsEx         = 01
	OsUserShift  = 6
	OsGroupShift = 3
	OsOthShift   = 0

	OsUserR   = OsRead << OsUserShift
	OsUserW   = OsWrite << OsUserShift
	OsUserX   = OsEx << OsUserShift
	OsUserRW  = OsUserR | OsUserW
	OsUserRWX = OsUserRW | OsUserX

	OsGroupR   = OsRead << OsGroupShift
	OsGroupW   = OsWrite << OsGroupShift
	OsGroupX   = OsEx << OsGroupShift
	OsGroupRW  = OsGroupR | OsGroupW
	OsGroupRWX = OsGroupRW | OsGroupX

	OsOthR   = OsRead << OsOthShift
	OsOthW   = OsWrite << OsOthShift
	OsOthX   = OsEx << OsOthShift
	OsOthRW  = OsOthR | OsOthW
	OsOthRWX = OsOthRW | OsOthX

	OsAllR   = OsUserR | OsGroupR | OsOthR
	OsAllW   = OsUserW | OsGroupW | OsOthW
	OsAllX   = OsUserX | OsGroupX | OsOthX
	OsAllRW  = OsAllR | OsAllW
	OsAllRWX = OsAllRW | OsGroupX
)

// BuffCache is a cache of the buffers we are in the middle of calculating
// Slightly dodgy, but we need to re-write the struicture to make this better
var BuffCache = medorg.NewCalcBuffer()

func fetchFile(potentialFileName string, dirStr string, fetchURL URL) {
	if fetchURL.URL() == "" {
		//fmt.Println("null fetch")
		return
	}
	// Create any directories needed to put this file in them
	out := createDownFile(dirStr, potentialFileName)
	if out == nil {
		return
	}

	timeout := time.Duration(FetchTimeout)
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Get(fetchURL.URL())
	if err != nil {
		err := out.Close()
		check(err)
		return
	}
	_ = DecodeHTTPError(err)
	var (
		trigger chan struct{}
		wg      *sync.WaitGroup
		reader  io.Reader
	)
	if true {
		var iw io.Writer
		iw, trigger = BuffCache.Calculate(potentialFileName)
		reader = io.TeeReader(resp.Body, iw)
		//defer close(trigger)
		//reader = resp.Body
	} else if GenChecksums {
		var iw io.Writer
		iw, trigger, wg = medorg.Calculator(potentialFileName)
		reader = io.TeeReader(resp.Body, iw)
		// defer wg.Wait()
		// defer close(trigger)

	} else {
		reader = resp.Body
	}
	// Read until EOF
	rc := logSlow(potentialFileName)
	_, _ = io.Copy(out, reader)
	defer log.Println("Finished downloading:", potentialFileName)
	// close(trigger)	// Trigger the Calculation completion
	close(rc)
	err = out.Close() // can't defer this because of the file sync needed.
	check(err)
	_ = resp.Body.Close()
	// Timestamp needs to be correct before this is closed
	if true {
		close(trigger)
	} else if GenChecksums {
		close(trigger)
		wg.Wait()
	}
}

func createDownFile(dirStr string, potentialFileName string) *os.File {
	var dirFileMode os.FileMode
	dirFileMode = os.ModeDir | (OsUserRWX | OsAllR)
	err := os.MkdirAll(dirStr, dirFileMode)
	check(err)

	out, err := os.Create(potentialFileName)
	if err != nil {
		var pe = err.(*os.PathError)        // let it panic or use the ,ok trick as below
		var en, ok = pe.Err.(syscall.Errno) // not a Go 1 Compat guarantee, so handle failed type assertion
		if !ok {
			log.Fatalf("Unexpected error from os.Create: %s\n", pe)
		}
		switch en {
		case syscall.EEXIST:
			log.Fatal("Error in os create, File does not exist")
		case syscall.EISDIR:
			// Malformed URL gives this
			// Indicates where we are fetching from is giving us illegal stuff
			return nil
		case syscall.EINVAL:
			log.Fatal("Error in os create, invalid name")
		case syscall.ENOENT:
			// No such file or directory
			return nil
		default:
			log.Fatalf("Unhandled syscall error:%x,%v\n", en, en)
		}
	}
	return out
}

func logSlow(fn string) chan struct{} {
	startTime := time.Now()
	closeChan := make(chan struct{})
	go func() {
		log.Println("Started downloading:\"", fn, "\"", " At:", startTime)
		for {
			select {
			case <-closeChan:
				return
			case <-time.After(time.Minute):
				log.Println("Still downloading:\"", fn, "\"", " After:", time.Since(startTime))
			}
		}
	}()
	return closeChan
}
func checkJpg(filename string) bool {
	out, err := os.Open(filename)
	check(err)
	defer func() {
		err := out.Close()
		check(err)
	}()
	_, err = jpeg.Decode(out)
	if err == nil {
	}
	switch err.(type) {
	case nil:
		return true
	case jpeg.FormatError:
		switch err.Error() {
		case "invalid JPEG format: bad RST marker":
			return false
		case "invalid JPEG format: short Huffman data":
			return false
		case "invalid JPEG format: missing SOI marker":
			return false
		case "invalid JPEG format: missing 0xff00 sequence":
			return false
		default:
			log.Fatalf("Unknown jpeg Error Text type:%T, Value %v\n", err, err)
		}
	default:
		switch err.Error() {
		case "unexpected EOF":
			return false
		case "EOF":
			return false
		default:
			log.Fatalf("Unknown jpeg Error type:%T, Value %v\n", err, err)
		}
	}
	return false
}
