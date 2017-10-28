package grab

import (
	"fmt"
	"io"

	"net/http"
	"net/url"
)

// HTTPError bundles up our error codes
type HTTPError int

// Express each http error I've come across
const (
	NIL = iota
	ErrBodyReadAfterClose
	ErrMissingFile
	ErrHeaderTooLong
	ErrShortBody
	ErrNotSupported
	ErrUnexpectedTrailer
	ErrMissingContentLength
	ErrNotMultipart
	ErrMissingBoundary
	EOF
	ErrURL
)

// DecodeHTTPError translates from the error we get back from http package
// to something we can use
func DecodeHTTPError(err error) HTTPError {
	if err == nil {
		return NIL
	}
	switch err {
	case http.ErrBodyReadAfterClose:
		fmt.Println("Read after close error")
		return ErrBodyReadAfterClose
	case http.ErrMissingFile:
		fmt.Println("Missing File")
		return ErrMissingFile
	case http.ErrHeaderTooLong:
		fmt.Println("ErrHeaderTooLong")
		return ErrMissingFile
	case http.ErrShortBody:
		fmt.Println("ErrShortBody")
		return ErrShortBody
	case http.ErrNotSupported:
		fmt.Println("ErrNotSupported")
		return ErrNotSupported
	case http.ErrUnexpectedTrailer:
		fmt.Println("ErrUnexpectedTrailer")
		return ErrUnexpectedTrailer
	case http.ErrMissingContentLength:
		fmt.Println("ErrMissingContentLength")
		return ErrMissingContentLength
	case http.ErrNotMultipart:
		fmt.Println("ErrNotMultipart")
		return ErrNotMultipart
	case http.ErrMissingBoundary:
		fmt.Println("ErrMissingBoundary")
		return ErrMissingBoundary
	case io.EOF:
		fmt.Println("EOF error found")
		return EOF
	default:
		switch err.(type) {
		case *url.Error:
			//fmt.Println("URL Error")
			return ErrURL
		default:
			fmt.Printf("Error type is %T, %#v\n", err, err)
			panic(err)
		}
	}
}
