package scanner

import (
	"errors"
	"testing"
)

func TestDefaultErrFunc(t *testing.T) {
	err := errors.New("parse error")
	line := []byte("bad line")
	if defaultErrFunc(line, err) != nil {
		t.Errorf("defaultErrFunc should return nil")
	}
}

func TestFoldErrFunc(t *testing.T) {
	err := errors.New("fold error")
	line := []byte("bad fold line")
	if foldErrFunc(line, err) != nil {
		t.Errorf("foldErrFunc should return nil")
	}
}

func TestParseOpts(t *testing.T) {
	o := parseOpts([]ScanOptT{})
	if o.maxSz != MaxRecordSize {
		t.Errorf("parseOpts default maxSz incorrect")
	}
	if o.stop != int64(^uint64(0)>>1) {
		t.Errorf("parseOpts default stop incorrect")
	}
	if o.errF == nil {
		t.Errorf("parseOpts default errF should not be nil")
	}

	o = parseOpts([]ScanOptT{WithFold(true)})
	if !o.fold {
		t.Errorf("WithFold should set fold true")
	}
	if o.errF == nil {
		t.Errorf("WithFold should set errF")
	}

	o = parseOpts([]ScanOptT{WithMaxSize(100)})
	if o.maxSz < 100 {
		t.Errorf("WithMaxSize should set maxSz >= 100")
	}

	o = parseOpts([]ScanOptT{WithStart(42)})
	if o.start != 42 {
		t.Errorf("WithStart should set start")
	}

	o = parseOpts([]ScanOptT{WithStop(99)})
	if o.stop != 99 {
		t.Errorf("WithStop should set stop")
	}

	o = parseOpts([]ScanOptT{WithMark(7)})
	if o.mark != 7 {
		t.Errorf("WithMark should set mark")
	}

	o = parseOpts([]ScanOptT{WithErrFunc(defaultErrFunc)})
	if o.errF == nil {
		t.Errorf("WithErrFunc should set errF")
	}
}
