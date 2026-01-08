package timez

import (
	"bytes"
	"errors"
	"regexp"
	"testing"
)

type errReader struct{}

func (e errReader) Read(p []byte) (int, error) {
	return 0, errors.New("boom")
}

func TestTryFormatsSelectsFirstMatchingSpec(t *testing.T) {
	specs := []FmtSpec{
		{
			Format:  FmtRfc3339,
			Pattern: `^NOT_A_TIMESTAMP`,
		},
		{
			Format:  TimestampFmt("2006-01-02 15:04:05"),
			Pattern: `^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})`,
		},
	}

	line := "2025-01-02 03:04:05 some message\n"
	factory, stamp := TryTimestampFormats(specs, []byte(line), DefaultSkip)
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}
	if stamp == 0 {
		t.Fatal("expected non-zero timestamp")
	}
}

func TestTryFormatsNoMatchReturnsZero(t *testing.T) {
	specs := []FmtSpec{
		{
			Format:  FmtRfc3339,
			Pattern: `^NOT_A_TIMESTAMP`,
		},
	}

	data := []byte("no timestamps here\njust text")
	factory, stamp := TryTimestampFormats(specs, data, DefaultSkip)
	if factory != nil {
		t.Fatalf("expected nil factory, got %v", factory)
	}
	if stamp != 0 {
		t.Fatalf("expected zero timestamp, got %d", stamp)
	}
}

func TestTryTimestampFormatsSkipBehavior(t *testing.T) {
	// Buffer where the first two lines are headers with no timestamp and the
	// third line contains the first valid RFC3339 timestamp.
	data := []byte("header line 1\nheader line 2\n2025-06-06T12:00:00Z real line\n")
	specs := []FmtSpec{
		{
			Format:  FmtRfc3339,
			Pattern: `^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)`,
		},
	}

	tests := []struct {
		name     string
		maxTries int
		wantHit  bool
	}{
		{"reach_valid_line", 2, true},
		{"skip_too_small", 1, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			factory, stamp := TryTimestampFormats(specs, data, tc.maxTries)
			if tc.wantHit {
				if factory == nil {
					t.Fatalf("expected non-nil factory when maxTries=%d", tc.maxTries)
				}
				if stamp == 0 {
					t.Fatalf("expected non-zero timestamp when maxTries=%d", tc.maxTries)
				}
			} else {
				if factory != nil {
					t.Fatalf("expected nil factory when maxTries=%d, got %v", tc.maxTries, factory)
				}
				if stamp != 0 {
					t.Fatalf("expected zero timestamp when maxTries=%d, got %d", tc.maxTries, stamp)
				}
			}
		})
	}
}

func TestDetectFormatReadError(t *testing.T) {
	_, _, err := DetectFormat(errReader{})
	if err == nil {
		t.Fatal("expected error from DetectFormat when reader fails")
	}
}

func TestDetectFormatUsesDefaultsSamples(t *testing.T) {
	// For each default spec, craft a sample log line that should
	// match its pattern and ensure DetectFormat can find a format
	// and the correct timestamp value. Samples are based on the
	// examples in detect.go comments and are ordered to match
	// the Defaults slice.
	tests := []struct {
		name  string
		stamp int64
		input string
	}{
		{
			"epochany_time_field",
			1744570895480541000,
			`{"level":"error","time":1744570895480541,"msg":"x"}`,
		},
		{
			"rfc3339_plain",
			1730797687000000000,
			"2024-11-05T09:08:07Z info message",
		},
		{
			"strimzi_kafka_topic_operator",
			1747498152465700000,
			"2025-05-17 16:09:12,46570 WARN  [vertx-blocked-thread-checker] BlockedThreadChecker: ",
		},
		{
			"iso_8601_tz_micros",
			1730822887123456000,
			"2024-11-05 09:08:07.123456-0700 msg",
		},
		{
			"iso_8601_millis",
			1730797687123000000,
			"2024-11-05 09:08:07.123 message",
		},
		{
			"w3c_postgres",
			1730797687000000000,
			"2024-11-05 09:08:07 connection received",
		},
		{
			"rfc3164_extended",
			1746056207715984000,
			"Apr 30 23:36:47.715984 WRN something",
		},
		{
			"rfc3164_basic",
			1767344887000000000,
			"Jan  2 09:08:07 host app[1]: msg",
		},
		{
			"klog",
			1762333687123456000,
			"I1105 09:08:07.123456 1234 somefile.go:10] message",
		},
		{
			"bracket_comma_millis",
			1730797687000000000,
			"[2024-11-05 09:08:07,000] message",
		},
		{
			"slash_datetime_24hr",
			1730797687000000000,
			"2024/11/05 09:08:07 something",
		},
		{
			"iis",
			1730797687000000000,
			"11/05/2024, 09:08:07 GET /index.html",
		},
		{
			"day_month_text_millis",
			1730797687000000000,
			"05 Nov 2024 09:08:07.000 message",
		},
		{
			"year_month_text_millis",
			1730797687000000000,
			"2024 Nov 05 09:08:07.000 message",
		},
		{
			"apache_style_millis",
			1730797687000000000,
			"05/Nov/2024:09:08:07.000 +0000 GET /",
		},
		{
			"us_12h_am_pm",
			1730848953000000000,
			"11/05/2024 11:22:33 PM message",
		},
		{
			"year_month_text",
			1730797687000000000,
			"2024 Nov 05 09:08:07 message",
		},
		{
			"json_timestamp_field",
			1742997662000000000,
			`{"timestamp":"2025-03-26T14:01:02Z","msg":"x"}`,
		},
		{
			"json_ts_field",
			1742997662000000000,
			`{"ts":"2025-03-26T14:01:02Z","msg":"x"}`,
		},
		{
			"nats",
			1745546464339092000,
			"[7] 2025/04/25 02:01:04.339092 [ERR] something",
		},
		{
			"k8s_creation_timestamp",
			1745441435000000000,
			`{"creationTimestamp":"2025-04-23T20:50:35Z"}`,
		},
		{
			"zap_dev",
			1745549708535000000,
			"2025-04-24T21:55:08.535-0500\tINFO message",
		},
		{
			"zap_prod",
			1745549708535518400,
			`{"level":"info","ts":1745549708.5355184,"msg":"x"}`,
		},
		{
			"loki",
			1741614760623431174,
			"ts=2025-03-10T13:52:40.623431174Z level=info msg=...",
		},
		{
			"datadog",
			1739383978715528000,
			`{"timestamp": "2025-02-12T18:12:58.715528Z", "event": "x"}`,
		},
		{
			"windows_events",
			1743448267142000000,
			`{"TimeCreated":"/Date(1743448267142)/"}`,
		},
		{
			"argocd",
			1739383978715528000,
			`time="2025-02-12T18:12:58.715528Z" level=info msg="x"`,
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := Defaults[i]
			re := regexp.MustCompile(spec.Pattern)
			matches := re.FindSubmatch([]byte(tc.input))
			if len(matches) <= 1 {
				t.Fatalf("pattern %q did not match sample %q", spec.Pattern, tc.input)
			}
			r := bytes.NewReader([]byte(tc.input + "\n"))
			factory, stamp, err := DetectFormat(r)
			if err != nil {
				t.Fatalf("DetectFormat returned error: %v", err)
			}
			if factory == nil {
				t.Fatal("expected non-nil factory")
			}
			if stamp != tc.stamp {
				t.Fatalf("timestamp mismatch: expected %d got %d", tc.stamp, stamp)
			}
		})
	}
}

func TestTryFormatsWithDefaultsNoMatch(t *testing.T) {
	data := []byte("no timestamps at all in this buffer")
	factory, stamp := TryTimestampFormats(Defaults, data, DefaultSkip)
	if factory != nil {
		t.Fatalf("expected nil factory, got %v", factory)
	}
	if stamp != 0 {
		t.Fatalf("expected zero timestamp, got %d", stamp)
	}
}

// Ensure DetectFormat handles small inputs that are shorter than detectSampleSize
// and thus cause io.ReadFull to return io.ErrUnexpectedEOF.
func TestDetectFormatShortInput(t *testing.T) {
	input := "2025-06-06T12:00:00Z short log line"
	buf := bytes.NewReader([]byte(input))
	factory, stamp, err := DetectFormat(buf)
	if err != nil {
		t.Fatalf("DetectFormat returned error for short input: %v", err)
	}
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}
	if stamp == 0 {
		t.Fatal("expected non-zero timestamp")
	}
}
