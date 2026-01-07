package timez

import (
	"bytes"
	"errors"
	"strconv"
	"time"

	"github.com/prequel-dev/prequel-logmatch/pkg/format"
	"github.com/rs/zerolog/log"
)

const (
	DefaultSkip = 50
)

var (
	ErrInvalidTimestampFormat = errors.New("invalid timestamp format")
)

type TimestampFmt string

func (f TimestampFmt) String() string {
	return string(f)
}

const (
	FmtRfc3339      TimestampFmt = "rfc3339"
	FmtRfc3339Nano  TimestampFmt = "rfc3339nano"
	FmtUnix         TimestampFmt = "unix"
	FmtEpochAny     TimestampFmt = "epochany"
	FmtEpochSeconds TimestampFmt = "epochseconds"
	FmtEpochMillis  TimestampFmt = "epochmillis"
	FmtEpochMicros  TimestampFmt = "epochmicros"
	FmtEpochNanos   TimestampFmt = "epochnanos"
	FmtDotNotation  TimestampFmt = "dotnotation"
)

func GetTimestampFormat(f TimestampFmt) (format.TimeFormatCbT, error) {

	switch f {
	case FmtRfc3339:
		return format.WithTimeFormat(time.RFC3339), nil
	case FmtRfc3339Nano:
		return format.WithTimeFormat(time.RFC3339Nano), nil
	case FmtUnix:
		return format.WithTimeFormat(time.UnixDate), nil
	case FmtEpochAny:
		return epochAny, nil
	case FmtEpochSeconds:
		return epochSeconds, nil
	case FmtEpochMillis:
		return epochMillis, nil
	case FmtEpochMicros:
		return epochMicros, nil
	case FmtEpochNanos:
		return epochNanos, nil
	case FmtDotNotation:
		return dotNotation, nil
	case "":
		return nil, ErrInvalidTimestampFormat
	default:
		return format.WithTimeFormat(string(f)), nil
	}
}

var (
	epochSeconds = epochParser(time.Second)
	epochMillis  = epochParser(time.Millisecond)
	epochMicros  = epochParser(time.Microsecond)
	epochNanos   = epochParser(time.Nanosecond)
)

func epochParser(unit time.Duration) format.TimeFormatCbT {
	return func(m []byte) (int64, error) {
		v, err := strconv.ParseInt(string(m), 10, 64)
		if err != nil {
			return 0, ErrInvalidTimestampFormat
		}
		return v * int64(unit), nil
	}
}

func epochAny(m []byte) (int64, error) {
	v, err := strconv.ParseInt(string(m), 10, 64)
	if err != nil {
		return 0, ErrInvalidTimestampFormat
	}

	sz := len(m)
	switch {
	case sz > 16:
		// NOOP: v *= int64(time.Nanosecond)
	case sz > 13:
		v *= int64(time.Microsecond)
	case sz > 10:
		v *= int64(time.Millisecond)
	default:
		v *= int64(time.Second)
	}
	return v, nil

}

func dotNotation(m []byte) (int64, error) {

	parts := bytes.SplitN(m, []byte("."), 2)

	secs, err := strconv.ParseInt(string(parts[0]), 10, 64)
	if err != nil {
		return 0, ErrInvalidTimestampFormat
	}

	var nsecs int64
	if len(parts) == 2 {
		fraction := parts[1]
		// Pad to nanoseconds
		for len(fraction) < 9 {
			fraction = append(fraction, '0')
		}
		if len(fraction) > 9 {
			fraction = fraction[:9]
		}
		nsecs, err = strconv.ParseInt(string(fraction), 10, 64)
		if err != nil {
			return 0, ErrInvalidTimestampFormat
		}
	}

	return secs*int64(time.Second) + nsecs, nil
}

// TryTimestampFormat attempts to parse the provided buffer using
// the given regex pattern and timestamp format.  It will try up to
// maxTries lines in the buffer to find a valid timestamp.
// On success, it returns the format factory and the parsed timestamp
// in nanoseconds since the epoch.
// On failure, it returns an error.
// Notes:
// - This function assumes timestamps are in UTC unless the format explicitly supports a timezone.
// 		TODO: Consider an option to specify a timezone or a local timezone.
// - This implementation is not optimized for performance; it is intended for use in
//   format detection where ease of use and correctness are prioritized.
//      TODO: Consider caching factories for repeated use with the same patterns/formats.

func TryTimestampFormat(exp string, fmtStr TimestampFmt, buf []byte, maxTries int) (format.FactoryI, int64, error) {

	var (
		ts      int64
		factory format.FactoryI
		cb      format.TimeFormatCbT
		err     error
	)

	if cb, err = GetTimestampFormat(fmtStr); err != nil {
		log.Warn().Err(err).Msg("Failed to get timestamp format")
		return nil, 0, err
	}

	if factory, err = format.NewRegexFactory(exp, cb); err != nil {
		log.Warn().Err(err).Msg("Failed to create regex factory")
		return nil, 0, err
	}

	f := factory.New()
	ts, err = f.ReadTimestamp(bytes.NewReader(buf))

	tries := 0
	for (err != nil || ts == 0) && tries < maxTries {
		// First line may contain a header; try up to N lines
		tries += 1
		if index := bytes.IndexByte(buf, '\n'); index != -1 {
			buf = buf[index+1:]
			ts, err = f.ReadTimestamp(bytes.NewReader(buf))
		} else {
			break
		}
	}

	if err != nil {
		return nil, 0, err
	}

	if ts == 0 {
		return nil, 0, ErrInvalidTimestampFormat
	}

	return factory, ts, nil
}

func TryTimestampFormats(specs []FmtSpec, data []byte, maxTries int) (factory format.FactoryI, stamp int64) {

	for _, spec := range specs {
		if factory, stamp, err := TryTimestampFormat(spec.Pattern, spec.Format, data, maxTries); err == nil {
			return factory, stamp
		}
	}

	return
}
