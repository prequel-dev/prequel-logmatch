package format

import (
	"errors"
	"testing"
	"time"
)

func TestReadCriEntry(t *testing.T) {

	const (
		shortTimestampS = "2016-10-06T00:17:09Z"
		longTimestampS  = "2018-10-06T00:17:09.669794202Z"
	)

	var (
		shortTimestamp, _ = time.Parse(time.RFC3339, shortTimestampS)
		longTimestamp, _  = time.Parse(time.RFC3339, longTimestampS)
	)

	tests := map[string]struct {
		data string
		want LogEntry
		werr error
	}{
		"empty":                  {data: "", werr: ErrNoTimestamp},
		"ts_no_delimeter":        {data: "2016", werr: ErrNoTimestamp},
		"ts_short1":              {data: "2016 ", werr: ErrParseTimestamp},
		"ts_short2":              {data: "2016-10-06T00:17:09 ", werr: ErrParseTimestamp},
		"ts_malformed":           {data: "2016-10-06T00:17:09.669794202z ", werr: ErrParseTimestamp},
		"ts_short_ok":            {data: shortTimestampS + " ", werr: ErrNoStreamType},
		"ts_ok_no_stream":        {data: longTimestampS + " ", werr: ErrNoStreamType},
		"malformed_stream":       {data: longTimestampS + "  ", werr: ErrUnknownStream},
		"stream_allcaps":         {data: longTimestampS + " STDOUT ", werr: ErrUnknownStream},
		"stdout_ok":              {data: longTimestampS + " stdout ", werr: ErrNoTag},
		"stderr_ok":              {data: longTimestampS + " stderr ", werr: ErrNoTag},
		"tag_no_delimeter":       {data: longTimestampS + " stdout P", werr: ErrNoTag},
		"tag_ok_empty_line":      {data: longTimestampS + " stdout P ", want: LogEntry{Timestamp: longTimestamp.UnixNano(), Stream: tokenStdout}},
		"tag_ok_empty_line2":     {data: longTimestampS + " stderr P ", want: LogEntry{Timestamp: longTimestamp.UnixNano(), Stream: tokenStderr}},
		"tag_complex_empty_line": {data: longTimestampS + " stderr P:FUNK ", want: LogEntry{Timestamp: longTimestamp.UnixNano(), Stream: tokenStderr}},
		"line_single_char":       {data: shortTimestampS + " stderr P:FUNK X", want: LogEntry{Timestamp: shortTimestamp.UnixNano(), Stream: tokenStderr, Line: "X"}},
		"line_with_escape":       {data: shortTimestampS + " stderr P:FUNK \\n", want: LogEntry{Timestamp: shortTimestamp.UnixNano(), Stream: tokenStderr, Line: "\\n"}},
	}

	var cf criFmtT

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := cf.ReadEntry([]byte(tc.data))
			if tc.want.Line != got.Line {
				t.Fatalf("expected line: %v, got: %v", tc.want.Line, got.Line)
			}
			if tc.want.Stream != got.Stream {
				t.Fatalf("expected stream: %v, got: %v", tc.want.Stream, got.Stream)
			}
			if tc.want.Timestamp != got.Timestamp {
				t.Fatalf("expected timestamp: %v, got: %v", tc.want.Timestamp, got.Timestamp)
			}
			if !errors.Is(err, tc.werr) {
				t.Fatalf("expected err: %v, got: %v", tc.werr, err)
			}
		})
	}

}
