package grab

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"

	"golang.org/x/net/html"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// CopyFile copies a file from src to dst. If src and dst files exist, and are
// the same, then return success. Otherise, attempt to create a hard link
// between the two files. If that fail, copy the file contents from src to dst.
func CopyFile(src, dst string) (err error) {
	sfi, err := os.Stat(src)
	if err != nil {
		return
	}
	if !sfi.Mode().IsRegular() {
		// cannot copy non-regular files (e.g., directories,
		// symlinks, devices, etc.)
		return fmt.Errorf("CopyFile: non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
	}
	dfi, err := os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("CopyFile: non-regular destination file %s (%q)", dfi.Name(), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			return
		}
	}
	if err = os.Link(src, dst); err == nil {
		return
	}
	err = copyFileContents(src, dst)
	return
}
func MoveFile(src, dst string) (err error) {
	err = CopyFile(src, dst)
	if err != nil {
		log.Fatalf("Copy problem\nType:%T\nVal:%v\n", err, err)
	}
	err = os.Remove(src)
	return err
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}

// Helper function to pull the href attribute from a Token
func getHref(t html.Token) (ok bool, href string) {
	// Iterate over all of the Token's attributes until we find an "href"
	for _, a := range t.Attr {
		if a.Key == "href" {
			href = a.Val
			ok = true
		}
	}

	// "bare" return will return the variables (ok, href) as defined in
	// the function definition
	return
}
func Readln(r *bufio.Reader) (string, error) {
	var (
		isPrefix bool  = true
		err      error = nil
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}

func LoadFile(filename string, the_chan chan Url, counter *OutCounter, close_chan, inc_count bool) {
	if counter != nil {
		defer counter.Dec()
	}
	if close_chan {
		defer close(the_chan)
	}
	if filename == "" {
		return
	}
	f, err := os.Open(filename)
	if err == os.ErrNotExist {
		return
	} else if err != nil {
		if os.IsNotExist(err) {
			return
		} else {
			fmt.Printf("error opening file: %T\n", err)
			os.Exit(1)
			return
		}
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for s, e := Readln(r); e == nil; s, e = Readln(r) {
		s = strings.TrimSpace(s)
		comment := strings.HasPrefix(s, "//")
		comment = comment || strings.HasPrefix(s, "#")
		if comment {
			continue
		}
		if s == "" {
			continue
		}
		//fmt.Println("Fast adding ", s)
		if inc_count {
			counter.Add()
		}
		tmp_u := NewUrl(s)
		if tmp_u.Initialise() {
			log.Fatal("nneded to init after new")
		}
		the_chan <- tmp_u
	}
}
func SaveFile(filename string, the_chan chan Url, counter *OutCounter) {
	if counter != nil {
		defer counter.Dec()
	}
	if filename == "" {
		return
	}
	f, err := os.Create(filename)
	check(err)
	defer f.Close()
	for v := range the_chan {
		fmt.Fprintf(f, "%s\n", v.Url())
	}
}
func LoadGob(filename string, the_chan chan Url, counter *OutCounter, close_chan, inc_count bool) {
	if counter != nil {
		defer counter.Dec()
	}
	if close_chan {
		defer close(the_chan)
	}
	if filename == "" {
		return
	}
	f, err := os.Open(filename)
	if err == os.ErrNotExist {
		return
	} else if err != nil {
		if os.IsNotExist(err) {
			return
		} else {
			fmt.Printf("error opening file: %T\n", err)
			os.Exit(1)
			return
		}
	}
	defer f.Close()
	fi, err := f.Stat()
	check(err)
	if fi.Size() > 1 {
		//fmt.Printf("Opened filename:%s,%d\n", filename, fi.Size())
		buff := make([]Url, 0)
		dec := gob.NewDecoder(f)
		err = dec.Decode(&buff)
		fmt.Printf("Gobbed in %d Items\n", len(buff))
		if err == io.EOF {
			log.Println("Gob Load error. Early EOF. Discarded Items")
			return
		} else if err != nil {
			log.Fatalf("Unexpected Gob Load Error, Type:%T,\nValue:%v", err, err)
		}
		for _, v := range buff {
			v.Initialise()
			the_chan <- v
		}

	}
}
func SaveGob(filename string, the_chan chan Url, counter *OutCounter) {
	if counter != nil {
		defer counter.Dec()
	}
	if filename == "" {
		return
	}
	if _, err := os.Stat(filename); err == nil {
		os.Remove(filename)
	}

	preader, pwriter := io.Pipe()

	f, err := os.Create(filename)
	check(err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer f.Close()
		defer wg.Done()
		last_read := false
		for !last_read {
			buf := make([]byte, 1<<10)
			//fmt.Println("reding into buffer")
			// REVISIT It would be more //el to read first outside the loop
			// Then we could pass the next buffer to the read while we wrote
			n, err := preader.Read(buf)
			//fmt.Println("Read from buffer",n,err)

			if err == io.EOF {
				fmt.Println("Detected end of file")
				last_read = true
			} else if err != nil {
				panic(err)
			}
			// Reslice so writer has the correct input
			buf = buf[:n]
			len_remaining := n
			for len_remaining > 0 {
				//fmt.Println("writing bytes:",len_remaining, len(buf), buf)
				n, err = f.Write(buf)
				//fmt.Println("Wrote",n,err)
				check(err)
				len_remaining -= n
			}
			if last_read {
				preader.Close()
				fmt.Println("Done and closing everything")
				return
			}
		}
	}()

	buff := make([]Url, 0)
	fmt.Println("Start bufferring")
	for v := range the_chan {
		buff = append(buff, v)
	}
	fmt.Printf("Gobbing Out %d Items\n", len(buff))
	enc := gob.NewEncoder(pwriter)

	err = enc.Encode(buff)
	fmt.Println("Encode complete, closing and waiting")
	pwriter.Close()
	wg.Wait()
	fmt.Println("Finished gobbing")
	check(err)

}
func GetBase(urls string) string {
	var ai *url.URL
	var err error
	ai, err = url.Parse(urls)
	check(err)
	hn := ai.Hostname()
	if hn == "http" || hn == "nats" || strings.Contains(hn, "+document.location.host+") {
		return ""
	} else {
		return hn
	}
}
