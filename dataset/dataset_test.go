package cookieDb

import (
	"bufio"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	f, err := os.Open("fixtures")
	f2, err := os.Open("fixtures")
	f3, err := os.Open("fixtures")
	defer f.Close()
	defer f2.Close()
	if err != nil {
		panic(err)
	}
	r := bufio.NewScanner(f)
	r2 := bufio.NewScanner(f2)
	r3 := bufio.NewScanner(f3)
	d1 := make(Intersection)
	d2 := make(CountTimeSet)
	d3 := make(StatSet)
	testCases := []struct {
		data *bufio.Scanner
		d    Shard
	}{
		{
			data: r,
			d:    &d1,
		},
		{
			data: r2,
			d:    &d2,
		},
		{
			data: r3,
			d:    &d3,
		},
	}
	for i, test := range testCases {
		d := FillDb(test.data, test.d, "test_2016111100.log")
		if d.Size() == 0 {
			t.Error("failed for test", test, i)
		}
		err := WriteShard("foo.gob", d)
		if err != nil {
			t.Error(err)
		}
		s, err := ReadShard("foo.gob")
		fmt.Println(s)
		if s.Size() == 0 {
			t.Error("failed for test", test, i)
		}
	}
}

var LINE = "BhrVPRR199e9aC8R	1479585192:142416,158621,158628,179159,287247,491863,491875;1480984187:407947"

func TestGetSession(t *testing.T) {
	fileName := "artefact_2016120601.log"
	ti := ParseTime(fileName)
	ses, cookieID := getSession([]byte(LINE), &ti)
	if len(ses.Events) != 2 {
		t.Error("len event != 2")
	}
	if cookieID != "BhrVPRR199e9aC8R" {
		t.Error("id is wrong")
	}
	fmt.Println(ses)
}

func TestEventHist(t *testing.T) {
	fileName := "artefact_2016120601.log"
	ti := ParseTime(fileName)
	fmt.Println(ti.In(time.UTC))
	fmt.Println("hour", ti.In(time.UTC).Hour())
	eventTime := time.Date(2016, 12, 6, 1, 1, 0, 0, LOC)
	e := Event{T: eventTime, Cats: []string{"1", "2"}}
	if e.Hist(&ti) {
		t.Error("not working")
	}
	eventTime = time.Date(2016, 12, 6, 0, 1, 0, 0, LOC)
	e = Event{T: eventTime, Cats: []string{"1", "2"}}
	if !e.Hist(&ti) {
		t.Error("not working")
	}
}

func TestEventCurrent(t *testing.T) {
	fileName := "artefact_2016120601.log"
	endTime := ParseTime(fileName).Add(time.Duration(time.Hour))
	startTime := endTime.Add(time.Duration(time.Hour * 12 * -1))
	eventTime := time.Date(2016, 12, 6, 1, 1, 0, 0, LOC)
	e := &Event{T: eventTime, Cats: []string{"1", "2"}}
	e.setCurrent(startTime, endTime)
	if !e.Current {
		t.Error("event is current", e.Current, e.T.In(LOC).Format(time.Stamp), startTime.In(LOC).Format(time.Stamp), endTime.In(LOC).Format(time.Stamp))
	}
	eventTime = time.Date(2016, 12, 6, 3, 1, 0, 0, LOC)
	e = &Event{T: eventTime, Cats: []string{"1", "2"}}
	e.setCurrent(startTime, endTime)
	if e.Current {
		t.Error("event is not current")
	}
	eventTime = time.Date(2016, 12, 5, 3, 1, 0, 0, LOC)
	e = &Event{T: eventTime, Cats: []string{"1", "2"}}
	e.setCurrent(startTime, endTime)
	if e.Current {
		t.Error("event is not current")
	}
}
