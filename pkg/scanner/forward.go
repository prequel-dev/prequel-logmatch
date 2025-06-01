package scanner

import (
	"bufio"
	"io"

	"github.com/prequel-dev/prequel-logmatch/internal/pkg/pool"
)

func ScanForward(rdr io.Reader, parseF ParseFuncT, scanF ScanFuncT, opts ...ScanOptT) error {

	var (
		buf     []byte
		o       = parseOpts(opts)
		scanner = bufio.NewScanner(rdr)
	)

	if o.maxSz == pool.MaxRecordSize {
		ptr := pool.PoolAlloc()
		defer pool.PoolFree(ptr)
		buf = *ptr
	} else {
		buf = make([]byte, o.maxSz)
	}

	scanF, errF, flushF := bindCallbacks(scanF, o)

	// Scanner will bail with bufio.ErrTooLong
	// if it encounters a line that is > o.maxSz.
	scanner.Buffer(buf, o.maxSz)

LOOP:
	for scanner.Scan() {

		entry, parseErr := parseF(scanner.Bytes())
		if parseErr != nil {
			if err := errF(scanner.Bytes(), parseErr); err != nil {
				return err
			}
			continue
		}

		if entry.Timestamp > o.stop {
			break LOOP
		}

		if scanF(entry) {
			break LOOP
		}
	}

	if flushF != nil {
		flushF()
	}

	return scanner.Err()
}

type flushFuncT func() bool

func bindCallbacks(scanF ScanFuncT, o scanOpt) (ScanFuncT, ErrFuncT, flushFuncT) {
	if !o.fold {
		return scanF, o.errF, nil
	}

	return bindFold(scanF, o.errF)
}
