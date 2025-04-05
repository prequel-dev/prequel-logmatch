package format

import (
	"bytes"
	"errors"
	"io"
	"time"

	"github.com/goccy/go-json"
)

type jsonCustomFmtT struct {
	path    *json.Path
	fmtTime string
}

type jsonCustomFactoryT struct {
	pathTime string
	fmtTime  string
}

func NewJsonCustomFactory(pathTime, fmtTime string) (FactoryI, error) {
	// Validate that the path is a valid JSON path
	_, err := json.CreatePath(pathTime)
	if err != nil {
		return nil, err
	}

	return &jsonCustomFactoryT{
		pathTime: pathTime,
		fmtTime:  fmtTime,
	}, nil
}

func (f *jsonCustomFactoryT) New() ParserI {
	// Path seems to have state so cannot reuse it; validate this.
	path, err := json.CreatePath(f.pathTime)
	if err != nil {
		panic(err) // This should never happen
	}

	return &jsonCustomFmtT{path: path, fmtTime: f.fmtTime}
}

func (f *jsonCustomFactoryT) String() string {
	return FactoryJSONCustom
}

func (f *jsonCustomFmtT) ReadTimestamp(rdr io.Reader) (ts int64, err error) {

	var (
		line    json.RawMessage
		decoder = json.NewDecoder(rdr)
	)

	if err = decoder.Decode(&line); err != nil {
		return
	}

	var stime string
	if err = f.path.Get(line, &stime); err != nil {
		err = errors.Join(ErrJsonTimeField, err)
		return
	}

	return f.parseTime(stime)
}

func (f *jsonCustomFmtT) parseTime(stime string) (ts int64, err error) {
	t, err := time.Parse(f.fmtTime, stime)
	if err != nil {
		err = errors.Join(ErrParseTimestamp, err)
		return
	}

	return t.UTC().UnixNano(), nil
}

// Read custom JSON Format
// TODO: Optimize this.  This is decoding the entire line when
// we only need the timestamp plus validation.

func (f *jsonCustomFmtT) ReadEntry(data []byte) (entry LogEntry, err error) {

	var (
		line    any
		decoder = json.NewDecoder(bytes.NewReader(data))
	)

	if err = decoder.Decode(&line); err != nil {
		err = errors.Join(ErrJsonUnmarshal, err)
		return
	}

	var stime string
	if err = f.path.Get(line, &stime); err != nil {
		err = errors.Join(ErrJsonTimeField, err)
		return
	}

	ts, err := f.parseTime(stime)
	if err != nil {
		return
	}

	entry.Line = string(data)
	entry.Timestamp = ts
	return
}
