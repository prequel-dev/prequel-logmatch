package format

import (
	"bufio"
	"errors"
	"io"

	"github.com/prequel-dev/prequel-logmatch/internal/pkg/pool"
)

type FactoryI interface {
	New() ParserI
	String() string
}

type ParserI interface {
	ReadTimestamp(rdr io.Reader) (int64, error)
	ReadEntry(line []byte) (LogEntry, error)
}

type DetectFormatFunc func(line []byte) (FactoryI, int64, error)

var supportedFormats = []DetectFormatFunc{
	detectJSON,
	detectCri,
	detectRFC3339Nano, // must come after detectCri since they both start with RFC3339Nano
}

const (
	FactoryJSON        = "json"
	FactoryRegex       = "regex"
	FactoryRfc3339Nano = "rfc3339Nano"
	FactoryJSONCustom  = "json_custom"
	FactoryCRI         = "cri"
)

const (
	DefBufferSize = 4 << 10 // 4K
	MaxRecordSize = pool.MaxRecordSize
)

func Detect(rdr io.Reader) (FactoryI, int64, error) {

	var (
		elist = []error{ErrFormatDetect}
		lr    = io.LimitReader(rdr, MaxRecordSize)
		bio   = bufio.NewReaderSize(lr, DefBufferSize)
	)

	line, err := bio.ReadBytes('\n')
	if err != nil {
		return nil, -1, err
	}

	for _, try := range supportedFormats {
		fmt, ts, err := try(line)
		if err == nil {
			return fmt, ts, err
		}
		elist = append(elist, err)
	}

	return nil, -1, errors.Join(elist...)
}
