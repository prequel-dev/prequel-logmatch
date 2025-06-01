package format

import (
	"bufio"
	"io"
	"regexp"
	"time"

	"github.com/prequel-dev/prequel-logmatch/internal/pkg/pool"
)

const defaultLineSize = 2048

type TimeFormatCbT func(m []byte) (int64, error)

type regexFmtT struct {
	expTime *regexp.Regexp
	cb      TimeFormatCbT
}

type regexFactoryT struct {
	expTime *regexp.Regexp
	cb      TimeFormatCbT
}

func WithTimeFormat(fmtTime string) TimeFormatCbT {
	return func(m []byte) (int64, error) {
		var (
			t   time.Time
			err error
		)

		if t, err = time.Parse(fmtTime, string(m)); err != nil {
			return 0, err
		}

		ts := t.UTC().UnixNano()

		// It is possible that the format does not have a year.  Check and adjust.
		if ts < 0 && t.Year() == 0 {
			ts = mungeYear(time.Now().UTC(), t)
		}

		return ts, nil
	}
}

func NewRegexFactory(expTime string, cb TimeFormatCbT) (FactoryI, error) {

	var (
		exp *regexp.Regexp
		err error
	)

	// Expression must compile
	if exp, err = regexp.Compile(expTime); err != nil {
		return nil, err
	}

	return &regexFactoryT{
		expTime: exp,
		cb:      cb,
	}, nil
}

func (f *regexFactoryT) New() ParserI {
	return &regexFmtT{expTime: f.expTime, cb: f.cb}
}

func (f *regexFactoryT) String() string {
	return FactoryRegex
}

func (f *regexFmtT) ReadTimestamp(rdr io.Reader) (ts int64, err error) {

	var (
		scanner = bufio.NewScanner(rdr)
		buffer  = make([]byte, defaultLineSize)
	)

	// Avoid using the pool buffer with scanner.Buffer.
	// When a pool buffer is set on the scanner, the buffer's full capacity
	// is used, not its size.  This causes the scanner to do a read of pool.MaxRecordSize
	// bytes, which is typically excessive for a single record read.
	// To avoid the allocation, we can consider a smaller memory pool at some point,
	// or find an alternative to bufio.Scanner that has more buffer control.
	scanner.Buffer(buffer, pool.MaxRecordSize)

	// Scanner will bail with bufio.ErrTooLong
	// if it encounters a line that is > pool.MaxRecordSize.
	if scanner.Scan() {
		m := f.expTime.FindSubmatch(scanner.Bytes())
		if len(m) <= 1 {
			err = ErrMatchTimestamp
			return
		}

		ts, err = f.cb(m[1])

	} else {
		err = scanner.Err()
	}

	return
}

// Read custom format
func (f *regexFmtT) ReadEntry(data []byte) (entry LogEntry, err error) {
	m := f.expTime.FindSubmatch(data)
	if len(m) <= 1 {
		err = ErrMatchTimestamp
		return
	}

	ts, err := f.cb(m[1])
	if err != nil {
		return
	}

	entry.Line = string(data)
	entry.Timestamp = ts
	return
}

// Year was not specified in the time format.
// Assume this year unless the time in in the future,
// in which case assume the previous year.

func mungeYear(now, t time.Time) int64 {
	var (
		dstYear  = now.Year()
		nowMonth = now.Month()
		tsMonth  = t.Month()
	)

	// If the timestamp month is in the future, then assume the year is the previous year.
	// We will do this calculation to month resolution, and add in some slop tolerance.
	// Could be more precise if necessary, but most logs that do not specify a year are short lived.
	switch {
	case tsMonth == nowMonth:
		// Normal case; the timestamp refers to this year.
	case tsMonth < nowMonth:
		// Timestamp from a previous month assume this year.
	case tsMonth > nowMonth && tsMonth-nowMonth > 1:
		// More than 11 months in the future is considered last year
		dstYear -= 1
	}

	nTime := time.Date(dstYear, tsMonth, t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	return nTime.UnixNano()
}
