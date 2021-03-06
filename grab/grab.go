package grab

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/html"
)

// URLMapBatchCnt defines how many batches to fetch in one visit read
const URLMapBatchCnt = 128

// URLMapBatchSize defines the number of items to fetch in a single read
const URLMapBatchSize = 128

func check(err error) {
	if err != nil {
		panic(err)
	}
}
func tempfilename(dirName string, create bool) string {
	if dirName == "" {
		dirName = os.TempDir()
	}
	tmpfile, err := ioutil.TempFile(dirName, "grabTemp_")
	check(err)
	filename := tmpfile.Name()
	err = tmpfile.Close()
	check(err)
	if !create {
		rmFilename(filename)
	}
	return filename
}
func fileExists(fn string) bool {
	_, err := os.Stat(fn)
	return err == nil
}

// IsDir returns true if the supplied path is a directory
func IsDir(path string) bool {
	if stat, err := os.Stat(path); err == nil && stat.IsDir() {
		return true
	}
	return false
}

func rmFilename(fn string) {
	if _, err := os.Stat(fn); err == nil {
		err = os.Remove(fn)
		check(err)
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

// MoveFile Implements a move function that works across file systems
// The inbuilt functions can struccle if hard links won't work
// i.e. you want to move between mount points
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
	defer func() {
		err := in.Close()
		check(err)
	}()
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
		if a.Key == "src" {
			href = a.Val
			ok = true
		}
	}

	return
}

// Readln read a line from a standard reader
func Readln(r *bufio.Reader) (string, error) {
	var (
		isPrefix = true
		err      error
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}

// LoadFile Load in a file of urls
// spitting them out on the supplied channel
// We may want to add one to a supplied counter (if we are tracking items added and processed)
// We may want to close the channel when we're done (or not)
// TBD incCount may be superflous
func LoadFile(filename string, theChan chan URL, counter *OutCounter, closeChan, incCount bool) {
	if counter != nil {
		defer counter.Dec()
	}
	if closeChan {
		defer close(theChan)
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
		}
		log.Fatalf("error opening URL file: %T\n", err)
		return
	}
	defer func() {
		err := f.Close()
		check(err)
	}()
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
		if incCount {
			counter.Add()
		}
		tmpU := NewURL(s)
		theChan <- tmpU
	}
}

// SaveFile reads urls comming in on the channel
// to a filename
// Optionally we can say when we're done
func SaveFile(filename string, theChan chan URL, counter *OutCounter) {
	if counter != nil {
		defer counter.Dec()
	}
	if filename == "" {
		return
	}
	f, err := os.Create(filename)
	check(err)
	defer func() {
		err := f.Close()
		check(err)
	}()
	for v := range theChan {
		fmt.Fprintf(f, "%s\n", v.URL())
	}
}

// LoadGob loads in the gob'd file of outstanding files to fetch
func LoadGob(filename string, theChan chan URL, counter *OutCounter, closeChan bool) {
	if counter != nil {
		defer counter.Dec()
	}
	if closeChan {
		defer close(theChan)
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
		}
		log.Fatalf("error opening GOB file: %T\n", err)
		return

	}
	defer func() {
		err := f.Close()
		check(err)
	}()
	fi, err := f.Stat()
	check(err)
	if fi.Size() > 1 {
		//fmt.Printf("Opened filename:%s,%d\n", filename, fi.Size())
		buff := make([]URL, 0)
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
			theChan <- v
		}

	}
}

// SaveGob - saves out supplied urls into a file
func SaveGob(filename string, theChan chan URL, counter *OutCounter) {
	if counter != nil {
		defer counter.Dec()
	}
	if filename == "" {
		return
	}
	rmFilename(filename)
	preader, pwriter := io.Pipe()

	f, err := os.Create(filename)
	check(err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			err := f.Close()
			check(err)
		}()
		defer wg.Done()
		lastRead := false
		for !lastRead {
			buf := make([]byte, 1<<10)
			//fmt.Println("reding into buffer")
			// REVISIT It would be more //el to read first outside the loop
			// Then we could pass the next buffer to the read while we wrote
			n, err := preader.Read(buf)
			//fmt.Println("Read from buffer",n,err)

			if err == io.EOF {
				fmt.Println("Detected end of file")
				lastRead = true
			} else if err != nil {
				panic(err)
			}
			// Reslice so writer has the correct input
			buf = buf[:n]
			lenRemaining := n
			for lenRemaining > 0 {
				//fmt.Println("writing bytes:",len_remaining, len(buf), buf)
				n, err = f.Write(buf)
				//fmt.Println("Wrote",n,err)
				check(err)
				lenRemaining -= n
			}
			if lastRead {
				check(preader.Close())
				fmt.Println("Done and closing everything")
				return
			}
		}
	}()

	buff := make([]URL, 0)
	fmt.Println("Start bufferring")
	for v := range theChan {
		buff = append(buff, v)
	}
	fmt.Printf("Gobbing Out %d Items\n", len(buff))
	enc := gob.NewEncoder(pwriter)

	err = enc.Encode(buff)
	check(err)
	fmt.Println("Encode complete, closing and waiting")
	err = pwriter.Close()
	check(err)
	wg.Wait()
	fmt.Println("Finished gobbing")

}

// Throughput returns an output on stats of our throughput
func Throughput(count int, elapsed float64) string {
	if count == 0 {
		return "Zero work\n"
	}
	tp := float64(count) / elapsed
	tpInt := int64(tp)
	if tpInt > 0 {
		return strconv.FormatInt(tpInt, 10) + " Items per Second\n"
	}
	tpIntMin := int64(tp * 60)
	if tpIntMin > 10 {
		return strconv.FormatInt(tpIntMin, 10) + " Items per Minute\n"
	}
	tpIntHour := int64(tp * 60 * 60)
	return strconv.FormatInt(tpIntHour, 10) + " Items per Hour\n"
}
