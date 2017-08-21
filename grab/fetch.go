package grab

import (
	"fmt"
	"image/jpeg"
	"io"
	"log"
	"net/http"
	"os"
	//"strings"
	"time"
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

const (
	OS_READ        = 04
	OS_WRITE       = 02
	OS_EX          = 01
	OS_USER_SHIFT  = 6
	OS_GROUP_SHIFT = 3
	OS_OTH_SHIFT   = 0

	OS_USER_R   = OS_READ << OS_USER_SHIFT
	OS_USER_W   = OS_WRITE << OS_USER_SHIFT
	OS_USER_X   = OS_EX << OS_USER_SHIFT
	OS_USER_RW  = OS_USER_R | OS_USER_W
	OS_USER_RWX = OS_USER_RW | OS_USER_X

	OS_GROUP_R   = OS_READ << OS_GROUP_SHIFT
	OS_GROUP_W   = OS_WRITE << OS_GROUP_SHIFT
	OS_GROUP_X   = OS_EX << OS_GROUP_SHIFT
	OS_GROUP_RW  = OS_GROUP_R | OS_GROUP_W
	OS_GROUP_RWX = OS_GROUP_RW | OS_GROUP_X

	OS_OTH_R   = OS_READ << OS_OTH_SHIFT
	OS_OTH_W   = OS_WRITE << OS_OTH_SHIFT
	OS_OTH_X   = OS_EX << OS_OTH_SHIFT
	OS_OTH_RW  = OS_OTH_R | OS_OTH_W
	OS_OTH_RWX = OS_OTH_RW | OS_OTH_X

	OS_ALL_R   = OS_USER_R | OS_GROUP_R | OS_OTH_R
	OS_ALL_W   = OS_USER_W | OS_GROUP_W | OS_OTH_W
	OS_ALL_X   = OS_USER_X | OS_GROUP_X | OS_OTH_X
	OS_ALL_RW  = OS_ALL_R | OS_ALL_W
	OS_ALL_RWX = OS_ALL_RW | OS_GROUP_X
)

func fetch_file(potential_file_name string, dir_str string, fetch_url Url) {
	// Create any directories needed to put this file in them
	var dir_file_mode os.FileMode
	dir_file_mode = os.ModeDir | (OS_USER_RWX | OS_ALL_R)
	os.MkdirAll(dir_str, dir_file_mode)

	out, err := os.Create(potential_file_name)
	switch t := err.(type) {
	case *os.PathError:
		switch t.Err {
		case os.ErrNotExist:
			log.Fatal("Invalid filename", potential_file_name)
		case os.ErrInvalid:
			log.Fatal("Invalid argument", potential_file_name)
		default:
			log.Fatalf("Que?\n\"%s\"\n%v\n,Dir:%s\nUrl:%s\n", potential_file_name, t.Err, dir_str, fetch_url)

		}
	case nil:
		// nothing
	default:
		log.Printf("Error is of type %T,n", err)
		check(err)

	}

	defer out.Close()

	if fetch_url.Url() == "" {
		//fmt.Println("null fetch")
		return
	}
	timeout := time.Duration(30 * time.Minute)
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Get(fetch_url.Url())
	// TBD Add error handling here
	if err != nil {
		return
	}
	_ = DecodeHttpError(err)
	defer resp.Body.Close()
	_, _ = io.Copy(out, resp.Body)
	//check(err)
}
func check_jpg(filename string) bool {
	out, err := os.Open(filename)
	check(err)
	defer out.Close()
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
		default:
			fmt.Printf("Unknown jpeg Error Text type:%T, Value %v\n", err, err)
			panic(err)
		}
	default:
		switch err.Error() {
		case "unexpected EOF":
			return false
		case "EOF":
			return false
		default:
			fmt.Printf("Unknown jpeg Error type:%T, Value %v\n", err, err)
			panic(err)
		}
	}
}
