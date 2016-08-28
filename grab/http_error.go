package grab

import (
	"fmt"
	"io"

	"net/http"
	"net/url"
)

type HttpError int

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
	URL
)

func DecodeHttpError(err error) HttpError {
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
			fmt.Println("URL Error")
			return URL
		default:
			fmt.Printf("Error type is %T, %#v\n", err, err)
			panic(err)
		}
	}
}
