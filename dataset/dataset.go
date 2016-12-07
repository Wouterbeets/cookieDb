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

//Shard abstracts the dataype that can depend on the analysis needed
type Shard interface {
	Add(line []byte) error
	Size() int
	Init()
	Type() string
	GetElems(nr int) []Cookie
	Get(cookieId string) Cookie
}

type Cookie interface {
	String() string
	Id() string
	Count() int
	Time() []time.Time
	Cats() []string
}

type cookieInter string

func (c cookieInter) String() string {
	return string(c)
}

func (c cookieInter) Id() string {
	return string(c)
}

func (c cookieInter) Count() int {
	return 1
}

func (c cookieInter) Cats() []string {
	return []string{""}
}

func (c cookieInter) Time() []time.Time {
	return nil
}

type cookieCountTime struct {
	id string
	Ct CountTime
}

func (c cookieCountTime) String() string {
	return c.id + fmt.Sprintln(c.Ct)
}

func (c cookieCountTime) Id() string {
	return c.id
}

func (c cookieCountTime) Cats() []string {
	return []string{""}
}

func (c cookieCountTime) Count() int {
	return c.Ct.Count
}

func (c cookieCountTime) Time() []time.Time {
	return c.Ct.TStamp
}

type CountTimeCats struct {
	Counter    int
	TStamp     []time.Time
	Categories []string
	CookieId   string
}

func (c *CountTimeCats) Count() int {
	return c.Counter
}

func (c *CountTimeCats) Time() []time.Time {
	return c.TStamp
}

func (c *CountTimeCats) Id() string {
	return c.CookieId
}

func (c *CountTimeCats) String() string {
	return fmt.Sprint(c.CookieId, c.Counter, c.TStamp, c.Categories)
}

func (c *CountTimeCats) Cats() []string {
	return c.Categories
}

type CountTimeCatsSet map[string]*CountTimeCats

func (set *CountTimeCatsSet) Add(line []byte) error {
	d := *set
	fields := getFields(line)
	cookieID := string(fields[0])
	c := &CountTimeCats{}
	for _, raw := range bytes.Split(fields[1], []byte(";")) {
		rawCats := bytes.Split(raw, []byte(":"))
		stamp, err := bytesconv.ParseInt(rawCats[0], 10, 64)
		cats := bytes.Split(rawCats[1], []byte(","))
		categories := []string{}
		for _, cat := range cats {
			categories = append(categories, string(cat))
		}
		if err != nil {
			log.Println(err)
		}
		unixStamp := time.Unix(stamp, 0)
		c.TStamp = append(c.TStamp, unixStamp)
		c.Counter = 1
		c.Categories = append(c.Categories, categories...)
		c.CookieId = cookieID
	}
	if cookie, ok := d[cookieID]; ok {
		cookie.TStamp = append(cookie.TStamp, c.TStamp...)
		cookie.Counter++
		cookie.Categories = append(cookie.Categories, c.Categories...)
		cookie.CookieId = cookieID
	} else {
		d[cookieID] = c
	}
	return nil
}

func (d *CountTimeCatsSet) String() string {
	s := ""
	i := 0
	for key, val := range *d {
		s += fmt.Sprint(key, " ", val.TStamp, " ", val.Categories)
		i++
		if i == 100 {
			break
		}
	}
	s += fmt.Sprintln()
	return s
}

func (d *CountTimeCatsSet) Type() string {
	return "CountTimeCatsSet"
}

func (d *CountTimeCatsSet) Size() int {
	return len(*d)
}

func (d *CountTimeCatsSet) Init() {
	*d = make(CountTimeCatsSet)
	fmt.Println(d.Size())
}

func (d *CountTimeCatsSet) GetElems(nr int) []Cookie {
	i := 0
	ret := make([]Cookie, 0, nr)
	for _, val := range *d {
		if i == nr {
			break
		}
		i++
		ret = append(ret, Cookie(val))
	}
	return ret
}

func (set *CountTimeCatsSet) Get(cookieId string) Cookie {
	d := *set
	if val, ok := d[cookieId]; ok {
		return Cookie(val)
	}
	return nil
}

type CountTimeSet map[string]*CountTime

func (set *CountTimeSet) Add(line []byte) error {
	d := *set
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
		c.Count = 1
	}
	if cookie, ok := d[cookieID]; ok {
		cookie.TStamp = append(cookie.TStamp, c.TStamp...)
		cookie.Count++
	} else {
		d[cookieID] = c
	}
	return nil
}

func (d *CountTimeSet) String() string {
	s := ""
	for key, val := range *d {
		s += fmt.Sprint(key, " ", val.TStamp, " ")
	}
	s += fmt.Sprintln()
	return s
}

func (d *CountTimeSet) Type() string {
	return "CountTimeSet"
}

func (d *CountTimeSet) Size() int {
	return len(*d)
}

func (d *CountTimeSet) Init() {
	*d = make(CountTimeSet)
	fmt.Println(d.Size())
}

func (d *CountTimeSet) GetElems(nr int) []Cookie {
	i := 0
	ret := make([]Cookie, 0, nr)
	for key, val := range *d {
		if i == nr {
			break
		}
		i++
		ret = append(ret, Cookie(cookieCountTime{key, *val}))
	}
	return ret
}

func (set *CountTimeSet) Get(cookieId string) Cookie {
	d := *set
	if val, ok := d[cookieId]; ok {
		return Cookie(cookieCountTime{cookieId, *val})
	}
	return nil
}

type CountTime struct {
	Count  int
	TStamp []time.Time
}

func (c CountTime) String() string {
	s := "["
	for _, t := range c.TStamp {
		s += t.Format(time.Stamp) + ", "
	}
	s += "]"
	return "\tcount: " + fmt.Sprint(c.Count) + "\ttimes: " + s
}

type Intersection map[string]struct{}

func (d *Intersection) Type() string {
	return "Intersection"
}

func (set *Intersection) Init() {
	*set = make(Intersection)
}
func (set *Intersection) Add(line []byte) error {
	d := *set
	fields := getFields(line)
	d[string(fields[0])] = struct{}{}
	return nil
}

func (d *Intersection) Size() int {
	return len(*d)
}

func (d *Intersection) String() string {
	return fmt.Sprint(map[string]struct{}(*d))
}

func (set *Intersection) GetElems(nr int) []Cookie {
	d := *set
	i := 0
	ret := make([]Cookie, 0, nr)
	for key, _ := range d {
		if i == nr {
			break
		}
		i++
		ret = append(ret, Cookie(cookieInter(key)))
	}
	return ret

}

func (set *Intersection) Get(cookieId string) Cookie {
	d := *set
	if _, ok := d[cookieId]; ok {
		return Cookie(cookieInter(cookieId))
	}
	return nil
}

func FillDb(scanner *bufio.Scanner, d Shard) Shard {
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
func ReadShard(shardName string) (Shard, error) {
	f, err := os.Open(shardName)
	if err != nil {
		return nil, err
	}
	dec := gob.NewDecoder(f)
	return readDecoder(dec)
}

func readDecoder(dec *gob.Decoder) (Shard, error) {
	var d Shard
	err := dec.Decode(&d)
	if err != nil {
		return d, err
	}
	return d, nil
}

//WriteShard writes the dataset to a file for later use
func WriteShard(fileName string, d Shard) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	_ = f
	enc := gob.NewEncoder(f)
	return writeToEncoder(enc, d)
}

func writeToEncoder(enc *gob.Encoder, d Shard) error {
	err := enc.Encode(&d)
	if err != nil {
		return err
	}
	return nil
}

func init() {
	gob.Register(&CountTimeSet{})
	gob.Register(&Intersection{})
	gob.Register(&CountTimeCatsSet{})
}
