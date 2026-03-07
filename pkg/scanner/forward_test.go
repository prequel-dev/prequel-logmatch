package scanner

import (
	"bufio"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prequel-dev/prequel-logmatch/pkg/format"
)

// Actual corrupted data
const corrupted = `
{"log":"[INFO] 10.244.0.201:40566 - 10187 \"A IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 136 0.000460728s\n","stream":"stdout","time":"2024-02-13T15:12:44.355965799Z"}
{"log":"[INFO] 10.244.0.201:56774 - 17984 \"A IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 136 0.000293089s\n","stream":"stdout","time":"2024-02-13T15:12:44.380212573Z"}
{"log":"[INFO] 10.244.0.201:51114 - 31602 \"AAAA IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 162 0.000114441s\n","stream":"stdout","time":"2024-02-13T15:24:09.841391183Z"}
{"log":"[INFO] 10.244.0.201:43521 - 42646 \"A IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 136 0.000128607s\n"{"log":"[INFO] 10.244.0.201:33715 - 27599 \"AAAA IN prequel-collector-service.pre
quel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 162 0.000115177s\n","stream":"stdout","time":"2024-02-13T15:24:09.856166904Z"}
{"log":"[INFO] 10.244.0.201:33686 - 15589 \"A IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 136 0.00014057s\n","stream":"stdout","time":"2024-02-13T15:24:09.856823439Z"}
{"log":"[INFO] 10.244.0.201:37826 - 38610 \"A IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 136 0.000075439s\n","stream":"stdout","time":"2024-02-13T15:24:09.857799908Z"}
{"log":"[INFO] 10.244.0.201:52267 - 22828 \"AAAA IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 162 0.000566709s\n","stream":"stdout","time":"2024-02-13T15:24:09.858265371Z"}
`

const extra = `
{"log":"[INFO] 10.244.0.201:37826 - 38610 \"A IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 136 0.000075439s\n","stream":"stdout","time":"2024-02-13T15:25:09.857799908Z"}
{"log":"[INFO] 10.244.0.201:52267 - 22828 \"AAAA IN prequel-collector-service.prequel.svc.cluster.local. udp 80 false 1232\" NOERROR qr,aa,rd 162 0.000566709s\n","stream":"stdout","time":"2024-02-13T15:26:09.858265371Z"}
`

func TestJsonCustomCorrupt(t *testing.T) {
	var (
		maxSz = 1024 * 1024
		sr    = NewStdReadScan(maxSz)
		rdr   = strings.NewReader(corrupted)
	)

	factory, err := format.NewJsonCustomFactory("$.time", time.RFC3339Nano)
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	f := factory.New()

	err = ScanForward(rdr, f.ReadEntry, sr.Scan, WithMaxSize(maxSz))

	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	res := sr.Result()

	if len(res.Logs) != 6 {
		t.Errorf("Expected %d entries, got %d", 6, len(res.Logs))
	}

	if res.Clip {
		t.Errorf("Expected no clip")
	}
}

func TestJsonCustomLogsExtraLF(t *testing.T) {

	var (
		maxSz = 1024 * 1024
		sr    = NewStdReadScan(maxSz)
		rdr   = strings.NewReader("\n\n\n" + corrupted + "\n\n\n" + extra + "\n\n")
	)

	factory, err := format.NewJsonCustomFactory("$.time", time.RFC3339Nano)
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	f := factory.New()

	err = ScanForward(rdr, f.ReadEntry, sr.Scan, WithMaxSize(maxSz))

	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	res := sr.Result()

	if len(res.Logs) != 8 {
		t.Errorf("Expected %d entries, got %d", 8, len(res.Logs))
	}

	if res.Clip {
		t.Errorf("Expected no clip")
	}
}

func TestJsonLogsCorrupt(t *testing.T) {
	var (
		factory = format.NewJsonFactory()
		f       = factory.New()
		maxSz   = 1024 * 1024
		sr      = NewStdReadScan(maxSz)
		rdr     = strings.NewReader(corrupted)
	)

	err := ScanForward(rdr, f.ReadEntry, sr.Bind(), WithMaxSize(maxSz))

	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	res := sr.Result()

	if len(res.Logs) != 6 {
		t.Errorf("Expected %d entries, got %d", 6, len(res.Logs))
	}

	if res.Clip {
		t.Errorf("Expected no clip")
	}
}

func TestJsonLogsExtraLF(t *testing.T) {

	var (
		factory = format.NewJsonFactory()
		f       = factory.New()
		maxSz   = 1024 * 1024
		sr      = NewStdReadScan(maxSz)
		rdr     = strings.NewReader("\n\n\n" + corrupted + "\n\n\n" + extra + "\n\n")
	)

	err := ScanForward(rdr, f.ReadEntry, sr.Scan, WithMaxSize(maxSz))

	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	res := sr.Result()

	if len(res.Logs) != 8 {
		t.Errorf("Expected %d entries, got %d", 8, len(res.Logs))
	}

	if res.Clip {
		t.Errorf("Expected no clip")
	}
}

func TestJsonLogsMaxSize(t *testing.T) {

	tests := map[string]struct {
		maxSz       int
		stop        int64
		wantErr     error
		wantLen     int
		wantUsed    int
		wantClipped bool
	}{
		"too small":         {maxSz: 10, wantClipped: false, wantErr: bufio.ErrTooLong},
		"one_less_output":   {maxSz: 177, wantClipped: true},
		"exact_output":      {maxSz: 178, wantLen: 1, wantUsed: 178, wantClipped: true},
		"one_less++":        {maxSz: 178 + 177, wantLen: 1, wantUsed: 178, wantClipped: true},
		"exact++":           {maxSz: 178 + 178, wantLen: 2, wantUsed: 178 + 178, wantClipped: true},
		"exact++_with_stop": {maxSz: 178 + 178, wantLen: 2, wantUsed: 178 + 178, wantClipped: false, stop: 1707837164380212573},
		"no_clip_on_EOF":    {maxSz: 1024 * 1024, wantLen: 6, wantUsed: 1073, wantClipped: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			factory := format.NewJsonFactory()
			f := factory.New()

			stop := tc.stop
			if stop == 0 {
				stop = math.MaxInt64
			}

			sr := NewStdReadScan(tc.maxSz)

			rdr := strings.NewReader(corrupted)

			err := ScanForward(rdr, f.ReadEntry, sr.Scan, WithMaxSize(tc.maxSz), WithStop(stop))

			if err != tc.wantErr {
				t.Errorf("Expected %v error got %v", tc.wantErr, err)
			}

			res := sr.Result()

			if len(res.Logs) != tc.wantLen {
				t.Errorf("Expected %d entries, got %d", tc.wantLen, len(res.Logs))
			}

			if res.Clip != tc.wantClipped {
				t.Errorf("Expected %v clip, got %v", tc.wantClipped, res.Clip)
			}

			if res.Sz != tc.wantUsed {
				t.Errorf("Expected %v BufUsed, got %v", tc.wantUsed, res.Sz)
			}
		})
	}
}

func TestForwardScanErrFuncExecutes(t *testing.T) {
	called := false
	customError := errors.New("custom error")

	errFunc := func(line []byte, err error) error {
		called = true
		return customError
	}
	parseF := func(line []byte) (LogEntry, error) {
		return LogEntry{}, io.ErrUnexpectedEOF
	}
	scanF := func(entry LogEntry) bool { return false }
	rdr := strings.NewReader("badline\n")
	// Use WithErrFunc to install the error function
	err := ScanForward(rdr, parseF, scanF, WithErrFunc(errFunc))
	if err != customError {
		t.Errorf("Expected %v error, got %v", customError, err)
	}
	if !called {
		t.Errorf("Error function was not called")
	}
}
