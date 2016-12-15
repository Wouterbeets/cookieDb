package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/wouterbeets/cookieDb/dataset"
	"io/ioutil"
	"log"
	"os"
	"time"
)

var sampleSize = flag.Int("sampleSize", 100, "number of cookies that we find information about throughout the dataset")
var countFlag = flag.Bool("count", true, "count the number of times a cookie exists in the data set")
var catFlag = flag.Bool("cat", true, "get the number of categories")
var times = flag.Bool("time", true, "show the timestamps for the user")
var all = flag.Bool("all", true, "show everything")
var firstDir = flag.String("intersection", "", "dir that holds the files to witch cookie ids to check the dataset for")
var thirdDir = flag.String("dataset", "", "dir that holds the files from which the data set should be created")
var timeFrame = flag.Int("timeFrame", 2, "Number of hours before the date in the name of the file that a cookie will be considered new data and not history")

type dataset struct {
	shards        []string
	sample        []cookieDb.Cookie
	sampleSize    int
	loadedShard   cookieDb.Shard
	loadedShardID string
}

func (s *dataset) setSample(size int) {
	s.sampleSize = size
	for i := 0; i < size; i++ {
		r := s.randomElement(s.shards[0])
		s.sample = append(s.sample, r)
	}
}

func (s *dataset) randomElement(shardName string) cookieDb.Cookie {
	s.loadShard(shardName)
	r := s.loadedShard.GetElems(1)
	return r[0]
}

func (s *dataset) loadShard(name string) {
	if s.loadedShardID != name {
		shard, err := cookieDb.ReadShard(name)
		if err != nil {
			log.Println("Error while loading shard", err)
		}
		s.loadedShard = shard
		s.loadedShardID = name
	}
}

type count struct {
	id    string
	count int
}

func (s *dataset) count() []count {
	counts := make([]count, s.sampleSize)
	for _, shard := range s.shards {
		s.loadShard(shard)
		for i, cookie := range s.sample {
			if c := s.loadedShard.Get(cookie.ID()); c != nil {
				counts[i].count += c.Count()
				counts[i].id = cookie.ID()
			}
		}
	}
	return counts
}

func (s *dataset) countTime() []cookieDb.CountTime {
	ct := make([]cookieDb.CountTime, s.sampleSize)
	for _, shard := range s.shards {
		s.loadShard(shard)
		for i, cookie := range s.sample {
			if c := s.loadedShard.Get(cookie.ID()); c != nil {
				ct[i].Count += c.Count()
				ct[i].TStamp = append(ct[i].TStamp, c.Time()...)
			}
		}
	}
	return ct
}

func (s *dataset) countTimeCats() []cookieDb.CountTimeCats {
	ct := make([]cookieDb.CountTimeCats, s.sampleSize)
	for _, shard := range s.shards {
		s.loadShard(shard)
		for i, cookie := range s.sample {
			if c := s.loadedShard.Get(cookie.ID()); c != nil {
				ct[i].Counter += c.Count()
				ct[i].TStamp = append(ct[i].TStamp, c.Time()...)
				ct[i].Categories = append(ct[i].Categories, c.Cats()...)
				ct[i].CookieID = cookie.ID()
			}
		}
	}
	return ct
}

func (s *dataset) all() []cookieDb.User {
	cookies := make([]cookieDb.User, s.sampleSize)
	for _, shard := range s.shards {
		s.loadShard(shard)
		for i, cookie := range s.sample {
			if c := s.loadedShard.Get(cookie.ID()); c != nil {
				cookies[i].Sess = append(cookies[i].Sess, c.User().Sess...)
				cookies[i].CookieID = cookie.ID()
			}
		}
	}
	return cookies
}

var errors *log.Logger

func main() {
	flag.Parse()
	interFileNames := []string{}
	_ = interFileNames
	datasetFileNames := []string{}
	if *firstDir != "" {
		interFileNames = fromDir(*firstDir)
	}
	if *thirdDir != "" {
		datasetFileNames = fromDir(*thirdDir)
	} else {
		datasetFileNames = flag.Args()
		if len(datasetFileNames) == 0 {
			panic("no dataset")
		}
	}
	var d cookieDb.Shard
	if *countFlag && *times && !*catFlag {
		set := make(cookieDb.CountTimeSet)
		d = &set
	} else if *all {
		set := make(cookieDb.StatSet)
		d = &set
	} else if *catFlag {
		set := make(cookieDb.CountTimeCatsSet)
		d = &set
	} else {
		set := make(cookieDb.Intersection)
		d = &set
	}
	set := makeShards(datasetFileNames, d)
	set.setSample(*sampleSize)
	c := set.all()
	endTime := cookieDb.ParseTime(datasetFileNames[0]).Add(time.Duration(time.Hour))
	startTime := endTime.Add(time.Duration(time.Hour * time.Duration(*timeFrame+1) * -1))
	f, err := os.Create("output.txt")
	if err != nil {
		errors.Fatal(err)
	}
	out := log.New(f, "", 0)
	count := 0
	for _, s := range c {
		if s.SetCurrent(startTime, endTime) {
			count++
		}
		out.Println(&s)
	}
	fmt.Println(float64(count) / float64(*sampleSize))
}

func shardAlreadyMade(shardName string) bool {
	if _, err := os.Stat(shardName); os.IsNotExist(err) {
		return false
	}
	return true
}

func makeShards(fileNames []string, d cookieDb.Shard) (set *dataset) {
	set = new(dataset)
	for _, name := range fileNames {
		shardName := name + "." + d.Type() + ".gob"
		if !shardAlreadyMade(shardName) {
			f, err := os.Open(name)
			if err != nil {
				panic(err)
			}
			d = cookieDb.FillDb(bufio.NewScanner(f), d, shardName)
			f.Close()
			if err := cookieDb.WriteShard(shardName, d); err != nil {
				log.Println(err)
			} else {
				set.shards = append(set.shards, shardName)
			}
			d.Init()
		} else {
			set.shards = append(set.shards, shardName)
		}
	}
	return
}

func fromDir(dir string) []string {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	filePaths := []string{}
	for _, fileInfo := range files {
		filePaths = append(filePaths, dir+"/"+fileInfo.Name())
	}
	return filePaths
}

func init() {
	f, err := os.Create("error.log")
	if err != nil {
		panic(err)
	}
	errors = log.New(f, "ERROR:", log.Lshortfile)
}
