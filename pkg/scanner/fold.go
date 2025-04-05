package scanner

import (
	"strings"
	"unicode/utf8"
)

func bindFold(scanF ScanFuncT, errF ErrFuncT) (ScanFuncT, ErrFuncT, flushFuncT) {

	var (
		pending LogEntry
		builder strings.Builder
	)

	nScanF := func(entry LogEntry) (done bool) {
		// Cache on first spin
		if pending.Timestamp == 0 {
			pending = entry
			return
		}

		if builder.Len() > 0 {
			pending.Line = builder.String()
			builder.Reset()
		}

		// Scan pending entry
		switch done = scanF(pending); done {
		case true:
			// Scan done; avoid emit on flush
			pending.Timestamp = 0
		default:
			// Promote current entry to pending
			pending = entry
		}

		return
	}

	// On error, append line to pending entry
	nErrF := func(line []byte, err error) error {
		// Relax error check; but validate utf8.
		// Occasionally binary garbage is seen in the log stream.
		if utf8.Valid(line) {
			if builder.Len() == 0 {
				builder.WriteString(pending.Line)
			}
			builder.Write(line)
		}
		return errF(line, err)
	}

	// On final flush, emit pending entry if exists
	nFlushF := func() (done bool) {
		if pending.Timestamp != 0 {
			if builder.Len() > 0 {
				pending.Line = builder.String()
				builder.Reset()
			}
			done = scanF(pending)
		}
		return
	}

	return nScanF, nErrF, nFlushF
}
