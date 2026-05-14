package timez

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/prequel-dev/prequel-logmatch/internal/pkg/munge"
	"github.com/prequel-dev/prequel-logmatch/pkg/format"
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

func TestDefaultsSamples(t *testing.T) {
	// For each default spec, craft a sample log line that should
	// match its pattern and ensure that the expected default spec
	// matches it and parses the expected timestamp.
	tests := []struct {
		name        string
		stamp       int64
		input       string
		expectIndex int
	}{
		{
			"epochany_time_field",
			1744570895480541000,
			`{"level":"error","time":1744570895480541,"msg":"x"}`,
			0,
		},
		{
			"rfc3339_plain",
			1730797687000000000,
			"2024-11-05T09:08:07Z info message",
			1,
		},
		{
			"strimzi_kafka_topic_operator",
			1747498152465700000,
			"2025-05-17 16:09:12,46570 WARN  [vertx-blocked-thread-checker] BlockedThreadChecker: ",
			2,
		},
		{
			"iso_8601_tz_micros",
			1730822887123456000,
			"2024-11-05 09:08:07.123456-0700 msg",
			3,
		},
		{
			"iso_8601_millis",
			1730797687123000000,
			"2024-11-05 09:08:07.123 message",
			4,
		},
		{
			"w3c_postgres",
			1730797687000000000,
			"2024-11-05 09:08:07 connection received",
			6,
		},
		{
			"rfc3164_extended",
			munge.MungeYear(time.Now(), time.Date(0, time.April, 30, 23, 36, 47, 715984000, time.UTC)),
			"Apr 30 23:36:47.715984 WRN something",
			7,
		},
		{
			"rfc3164_basic",
			1767344887000000000,
			"Jan  2 09:08:07 host app[1]: msg",
			8,
		},
		{
			"klog",
			1762333687123456000,
			"I1105 09:08:07.123456 1234 somefile.go:10] message",
			9,
		},
		{
			"bracket_comma_millis",
			1730797687000000000,
			"[2024-11-05 09:08:07,000] message",
			10,
		},
		{
			"slash_datetime_24hr",
			1730797687000000000,
			"2024/11/05 09:08:07 something",
			11,
		},
		{
			"iis",
			1730797687000000000,
			"11/05/2024, 09:08:07 GET /index.html",
			12,
		},
		{
			"day_month_text_millis",
			1730797687000000000,
			"05 Nov 2024 09:08:07.000 message",
			13,
		},
		{
			"year_month_text_millis",
			1730797687000000000,
			"2024 Nov 05 09:08:07.000 message",
			14,
		},
		{
			"apache_style_millis",
			1730797687000000000,
			"05/Nov/2024:09:08:07.000 +0000 GET /",
			15,
		},
		{
			"us_12h_am_pm",
			1730848953000000000,
			"11/05/2024 11:22:33 PM message",
			16,
		},
		{
			"year_month_text",
			1730797687000000000,
			"2024 Nov 05 09:08:07 message",
			17,
		},
		{
			"json_timestamp_field",
			1742997662000000000,
			`{"timestamp":"2025-03-26T14:01:02Z","msg":"x"}`,
			18,
		},
		{
			"json_ts_field",
			1742997662000000000,
			`{"ts":"2025-03-26T14:01:02Z","msg":"x"}`,
			18,
		},
		{
			"nats",
			1745546464339092000,
			"[7] 2025/04/25 02:01:04.339092 [ERR] something",
			19,
		},
		{
			"k8s_creation_timestamp",
			1745441435000000000,
			`{"creationTimestamp":"2025-04-23T20:50:35Z"}`,
			18,
		},
		{
			"zap_dev",
			1745549708535000000,
			"2025-04-24T21:55:08.535-0500\tINFO message",
			20,
		},
		{
			"zap_prod",
			1745549708535518400,
			`{"level":"info","ts":1745549708.5355184,"msg":"x"}`,
			21,
		},
		{
			"loki",
			1741614760623431174,
			"ts=2025-03-10T13:52:40.623431174Z level=info msg=...",
			22,
		},
		{
			"datadog",
			1739383978715528000,
			`{"timestamp": "2025-02-12T18:12:58.715528Z", "event": "x"}`,
			18,
		},
		{
			"windows_events",
			1743448267142000000,
			`{"TimeCreated":"/Date(1743448267142)/"}`,
			23,
		},
		{
			"argocd",
			1739383978715528000,
			`time="2025-02-12T18:12:58.715528Z" level=info msg="x"`,
			24,
		},
		{
			"redpanda",
			1749523274429000000,
			`2025-06-10 02:41:14,429 - INFO - Closing connection [IPv6 ('::1', 9092, 0, 0)]`,
			25,
		},
		{
			"neutron",
			1737371201655000000,
			`2025-01-20T11:06:41.655Z|114883|poll_loop|INFO|wakeup due to [POLLOUT] on fd 56 (10.15.58.43:6642<->10.15.52.89:43910) at ../lib/stream-fd.c:153 (52% CPU usage`,
			1,
		},
		{
			"kubernetes",
			1756300707000000000,
			`2025-08-27T13:18:27Z	cre-demo/cmd-127	bad-cmd	Error	127`,
			1,
		},
		{
			"terraform",
			1737560180978000000,
			`2025-01-22 15:36:20,978: ERROR/ForkPoolWorker-6] Failed to create Terraform Cloud workspace for HIM deployment`,
			25,
		},
		{
			"istiod",
			1750536672746046000,
			`2025-06-21T20:11:12.746046Z	state	timed out waiting for workload 'default.default (curlpod)' from xds`,
			1,
		},
		{
			"redpanda2",
			1749206738150000000,
			`2025-06-06 16:15:38.150+05:30 [node_id=1] [subsystem=health_manager] [level=CRITICAL] Cluster health degraded: Multiple nodes unresponsive or reporting critical errors. Controller quorum lost. Data availability severely impacted.`,
			26,
		},
		{
			"rabbit",
			1741701905303788000,
			`2025-03-11 14:05:05.303788+00:00 [noti] <0.60.0>`,
			5,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			// Try the default list in order as this is how DetectFormat will do it.
			// If the correct spec is found, we know the pattern matched and the timestamp was parsed correctly.
			var (
				err     error
				factory format.FactoryI
				ts      int64
				index   int
			)
			for i, spec := range Defaults {

				if factory, ts, err = TryTimestampFormat(spec.Pattern, spec.Format, []byte(tc.input), DefaultSkip); err == nil {
					index = i
					break
				}
			}

			if err != nil {
				t.Fatalf("DetectFormat returned error: %v", err)
			}

			if index != tc.expectIndex {
				t.Errorf("matched wrong spec: expected index %d but got %d", tc.expectIndex, index)
			}

			if factory == nil {
				t.Fatal("expected non-nil factory")
			}

			if ts != tc.stamp {
				sExpected := time.Unix(0, tc.stamp).UTC().Format(time.RFC3339Nano)
				sGot := time.Unix(0, ts).UTC().Format(time.RFC3339Nano)
				t.Fatalf("timestamp mismatch: expected %d(%s) got %d(%s)", tc.stamp, sExpected, ts, sGot)
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
