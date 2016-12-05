package cookieDb

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/objconv/bytesconv"
)

//Dataset abstracts the dataype that can depend on the analysis needed
type Dataset interface {
	Add(line []byte) error
	Size() int
}

type CountTimeSet map[string]*CountTime

func (d CountTimeSet) Add(line []byte) error {
	fields := getFields(line)
	cookieID := string(fields[0])
	c := &CountTime{}
	for _, raw := range bytes.Split(fields[1], []byte(";")) {
		rawStamp := bytes.Split(raw, []byte(":"))[0]
		stamp, err := bytesconv.ParseInt(rawStamp, 10, 64)
		if err != nil {
			log.Println(err)
		}
		unixStamp := time.Unix(stamp, 0)
		c.TStamp = append(c.TStamp, unixStamp)
	}
	if cookie, ok := d[cookieID]; ok {
		cookie.TStamp = append(cookie.TStamp, c.TStamp...)
	} else {
		d[cookieID] = c
	}
	return nil
}

func (d CountTimeSet) String() string {
	s := ""
	for key, val := range d {
		s += fmt.Sprint(key, " ", val.TStamp, " ")
	}
	s += fmt.Sprintln()
	return s
}

func (d CountTimeSet) Size() int {
	return len(d)
}

type CountTime struct {
	Count  int
	TStamp []time.Time
}

type Intersection map[string]struct{}

func (d Intersection) Add(line []byte) error {
	fields := getFields(line)
	d[string(fields[0])] = struct{}{}
	return nil
}

func (d Intersection) Size() int {
	return len(d)
}

func (d Intersection) String() string {
	return fmt.Sprint(map[string]struct{}(d))
}

func fillDb(scanner *bufio.Scanner, d Dataset) Dataset {
	for scanner.Scan() {
		d.Add(scanner.Bytes())
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading file input:", err)
	}
	return d
}

func getFields(line []byte) [][]byte {
	return bytes.Split(line, []byte("\t"))
}

//ReadShard reads the file pointed to by shardName and returns the it as a dataset
func ReadShard(shardName string) (Dataset, error) {
	f, err := os.Open(shardName)
	if err != nil {
		return nil, err
	}
	dec := gob.NewDecoder(f)
	return readDecoder(dec)
}

func readDecoder(dec *gob.Decoder) (Dataset, error) {
	var d Dataset
	err := dec.Decode(&d)
	if err != nil {
		return d, err
	}
	return d, nil
}

//WriteShard writes the dataset to a file for later use
func WriteShard(fileName string, d Dataset) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	_ = f
	enc := gob.NewEncoder(f)
	return writeToEncoder(enc, d)
}

func writeToEncoder(enc *gob.Encoder, d Dataset) error {
	err := enc.Encode(&d)
	if err != nil {
		fmt.Println("fuck")
		return err
	}
	fmt.Println("no error")
	return nil
}

func init() {
	gob.Register(CountTimeSet{})
	gob.Register(Intersection{})
}
