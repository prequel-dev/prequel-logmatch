package format

import (
	"bytes"
	"errors"
	"io"
	"time"

	"github.com/prequel-dev/prequel-logmatch/internal/pkg/pool"
)

type rfc339NanoFmtT struct {
}

type rfc339NanoFactoryT struct {
}

func (f *rfc339NanoFactoryT) New() ParserI {
	return &rfc339NanoFmtT{}
}

func (f *rfc339NanoFactoryT) String() string {
	return "rfc339Nano"
}

func (f *rfc339NanoFmtT) ReadTimestamp(rdr io.Reader) (ts int64, err error) {

	ptr := pool.PoolAlloc()
	defer pool.PoolFree(ptr)
	buf := (*ptr)[:tsBufSize]

	n, err := io.ReadFull(rdr, buf)
	switch err {
	case nil:
	case io.EOF, io.ErrUnexpectedEOF:
		if n == 0 {
			return
		}
	default:
		return
	}

	return scanCriTimestamp(buf[:n])
}

// Expects format:
//	2016-10-06T00:17:09.669794202Z log content 1
//	2016-10-06T00:17:09.669794203Z log content 2

func (f *rfc339NanoFmtT) ReadEntry(line []byte) (entry LogEntry, err error) {

	idx := bytes.IndexByte(line, delimiter)
	if idx < 0 {
		entry = LogEntry{}
		err = ErrNoTimestamp
		return
	}

	ts, err := time.Parse(time.RFC3339Nano, string(line[:idx]))
	if err != nil {
		err = errors.Join(ErrParseTimestamp, err)
		return
	}

	entry.Timestamp = ts.UnixNano()

	entry.Line = string(line[idx+1:])

	return
}

func detectRFC339Nano(line []byte) (FactoryI, int64, error) {

	var cf rfc339NanoFmtT
	entry, err := cf.ReadEntry(line)

	if err != nil {
		return nil, -1, err
	}

	return &rfc339NanoFactoryT{}, entry.Timestamp, nil
}
