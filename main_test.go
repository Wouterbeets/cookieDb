package cookieDb

import (
	"bufio"
	"fmt"
	"os"
	"testing"
)

func TestParse(t *testing.T) {
	f, err := os.Open("fixtures")
	f2, err := os.Open("fixtures")
	defer f.Close()
	defer f2.Close()
	if err != nil {
		panic(err)
	}
	r := bufio.NewScanner(f)
	r2 := bufio.NewScanner(f2)
	testCases := []struct {
		data *bufio.Scanner
		d    Dataset
	}{
		{
			data: r,
			d:    make(Intersection),
		},
		{
			data: r2,
			d:    make(CountTimeSet),
		},
	}
	for i, test := range testCases {
		d := fillDb(test.data, test.d)
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
