package format

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/prequel-dev/prequel-logmatch/internal/pkg/pool"
)

const (
	delimiter   = ' '
	pageSize    = 4096
	tsBufSize   = 64
	avgLogSize  = 256
	tokenStdout = "stdout"
	tokenStderr = "stderr"
)

var (
	sliceStdout = []byte(tokenStdout)
	sliceStderr = []byte(tokenStderr)
)

type criFmtT struct {
}

type criFactoryT struct {
}

func (f *criFactoryT) New() ParserI {
	return &criFmtT{}
}

func (f *criFactoryT) String() string {
	return FactoryCRI
}

func (f *criFmtT) ReadTimestamp(rdr io.Reader) (ts int64, err error) {

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

// Read CRI Entry line
// (see https://github.com/kubernetes/kubernetes/blob/v1.29.1/pkg/kubelet/kuberuntime/logs/logs.go#L129)
// Expects format:
//	2016-10-06T00:17:09.669794202Z stdout P log content 1
//	2016-10-06T00:17:09.669794203Z stderr F log content 2

func (f *criFmtT) ReadEntry(line []byte) (entry LogEntry, err error) {

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

	line = line[idx+1:]
	idx = bytes.IndexByte(line, delimiter)
	if idx < 0 {
		entry = LogEntry{}
		err = ErrNoStreamType
		return
	}

	// Avoid extra allocation of string to hold stream;
	// Use existing const which is copied as a reference.
	stream := line[:idx]
	switch {
	case bytes.Equal(stream, sliceStdout):
		entry.Stream = tokenStdout
	case bytes.Equal(stream, sliceStderr):
		entry.Stream = tokenStderr
	default:
		entry = LogEntry{}
		err = fmt.Errorf("unknown stream %s: %w", stream, ErrUnknownStream)
		return
	}

	// Search past tag; we are ignoring for now
	line = line[idx+1:]
	idx = bytes.IndexByte(line, delimiter)
	if idx < 0 {
		entry = LogEntry{}
		err = ErrNoTag
		return
	}

	entry.Line = string(line[idx+1:])
	return
}

func scanCriTimestamp(buf []byte) (int64, error) {
	offset := bytes.IndexByte(buf, delimiter)

	if offset < 0 {
		return -1, ErrNoTimestamp
	}

	ts, err := time.Parse(time.RFC3339Nano, string(buf[:offset]))
	if err != nil {
		return -1, err
	}
	return ts.UnixNano(), nil
}

func detectCri(line []byte) (FactoryI, int64, error) {

	var cf criFmtT
	entry, err := cf.ReadEntry(line)

	if err != nil {
		return nil, -1, err
	}

	return &criFactoryT{}, entry.Timestamp, nil
}
