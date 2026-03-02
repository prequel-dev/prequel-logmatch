package format

import (
	"bufio"
	"io"
	"regexp"
	"time"

	"github.com/prequel-dev/prequel-logmatch/internal/pkg/pool"
)

const (
	defaultLineSize  = 2048
	defaultMungeSlop = time.Hour * 24 * 7
)

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

// Munge the year to be the closest to now, but not more than futureSlop in the future.
func mungeYear(now, t time.Time) int64 {
	return mungeYearWithSlop(now, t, defaultMungeSlop)
}

// mungeYearWithSlop assigns a year to a timestamp `t` that has no year specified.
// It chooses the closest year to `now`, allowing the resulting time to be up to `futureSlop`
// in the future relative to `now`.
//
// Behavior:
//   - Interpret `t` using the same month, day, and time, but choose a year that makes it
//     as close as possible to `now`, while not placing it more than `futureSlop` into the future.
//   - If choosing the current year would put `t` more than `futureSlop` ahead of `now`,
//     the year is adjusted (e.g. to the previous or next year) to satisfy this constraint.
//   - A negative `futureSlop` will cause a panic.
//
// Example:
//
//	now        = Dec 31, 2026 23:00
//	t          = Jan 6 10:00        // no year specified
//	futureSlop = 7*24*time.Hour
//	→ returns Jan 6, 2027 10:00
func mungeYearWithSlop(now, t time.Time, futureSlop time.Duration) int64 {
	if futureSlop < 0 {
		panic("futureSlop cannot be negative")
	}

	// Compute the reference time including slop
	nowWithSlop := now.Add(futureSlop).UTC()

	// Force UTC
	t = t.UTC()

	// Build candidate timestamp using the year of nowWithSlop
	candidate := time.Date(
		nowWithSlop.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		time.UTC,
	)

	// Adjust candidate if it falls outside the allowed window
	// Positive slop: timestamps too far in the future belong to last year
	if candidate.After(nowWithSlop) {
		candidate = candidate.AddDate(-1, 0, 0)
	}

	return candidate.UTC().UnixNano()
}
