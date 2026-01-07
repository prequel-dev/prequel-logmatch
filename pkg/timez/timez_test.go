package timez

import (
	"errors"
	"testing"
	"time"
)

func TestGetTimestampFormat(t *testing.T) {
	cb, err := GetTimestampFormat(FmtRfc3339)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ts, err := cb([]byte("2025-01-02T03:04:05Z"))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	want := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC).UnixNano()
	if ts != want {
		t.Fatalf("expected %d got %d", want, ts)
	}
}

func TestEpochParserAndAny(t *testing.T) {
	cb, _ := GetTimestampFormat(FmtEpochMillis)
	ts, err := cb([]byte("42"))
	if err != nil || ts != 42*int64(time.Millisecond) {
		t.Fatalf("epoch millis failed")
	}

	cb, _ = GetTimestampFormat(FmtEpochAny)
	ts, err = cb([]byte("1000"))
	if err != nil || ts != 1000*int64(time.Second) {
		t.Fatalf("epoch any failed")
	}
}

func TestEpochParsers(t *testing.T) {
	tests := []struct {
		name  string
		fmt   TimestampFmt
		input string
		want  int64
	}{
		{"seconds", FmtEpochSeconds, "2", 2 * int64(time.Second)},
		{"millis", FmtEpochMillis, "3", 3 * int64(time.Millisecond)},
		{"micros", FmtEpochMicros, "4", 4 * int64(time.Microsecond)},
		{"nanos", FmtEpochNanos, "5", 5 * int64(time.Nanosecond)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cb, err := GetTimestampFormat(tc.fmt)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got, err := cb([]byte(tc.input))
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected %d got %d", tc.want, got)
			}
		})
	}

	cb, _ := GetTimestampFormat(FmtEpochMillis)
	if _, err := cb([]byte("not-a-number")); !errors.Is(err, ErrInvalidTimestampFormat) {
		t.Fatalf("expected ErrInvalidTimestampFormat, got %v", err)
	}
}

func TestEpochAnyUnitsAndErrors(t *testing.T) {
	cb, _ := GetTimestampFormat(FmtEpochAny)

	tests := []struct {
		name  string
		input string
		want  int64
	}{
		{"seconds", "1000", 1000 * int64(time.Second)},
		{"millis", "12345678901", 12345678901 * int64(time.Millisecond)},
		{"micros", "12345678901234", 12345678901234 * int64(time.Microsecond)},
		{"nanos", "1234567890123456789", 1234567890123456789},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cb([]byte(tc.input))
			if err != nil {
				t.Fatalf("epochAny failed: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected %d got %d", tc.want, got)
			}
		})
	}

	if _, err := cb([]byte("not-a-number")); !errors.Is(err, ErrInvalidTimestampFormat) {
		t.Fatalf("expected ErrInvalidTimestampFormat, got %v", err)
	}
}

func TestTryTimestampFormat(t *testing.T) {
	line := "2025-06-06T12:00:00Z first line\nsecond" // newline ensures only first line used
	factory, ts, err := TryTimestampFormat(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)`, FmtRfc3339, []byte(line), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if factory == nil {
		t.Fatal("expected factory")
	}
	want := time.Date(2025, 6, 6, 12, 0, 0, 0, time.UTC).UnixNano()
	if ts != want {
		t.Fatalf("timestamp mismatch")
	}
}

func TestTryTimestampFormatSkipsHeader(t *testing.T) {
	buf := "header without timestamp\n2025-06-06T12:00:00Z real line\n"
	factory, ts, err := TryTimestampFormat(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)`, FmtRfc3339, []byte(buf), DefaultSkip)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if factory == nil {
		t.Fatal("expected factory")
	}
	want := time.Date(2025, 6, 6, 12, 0, 0, 0, time.UTC).UnixNano()
	if ts != want {
		t.Fatalf("timestamp mismatch: want %d got %d", want, ts)
	}
}

func TestTryTimestampFormatInvalidRegex(t *testing.T) {
	factory, ts, err := TryTimestampFormat("(", FmtRfc3339, []byte("2025-06-06T12:00:00Z"), 1)
	if err == nil {
		t.Fatal("expected error for invalid regex")
	}
	if factory != nil || ts != 0 {
		t.Fatalf("expected nil factory and zero ts, got %v, %d", factory, ts)
	}
}

func TestTryTimestampFormatNoTimestamp(t *testing.T) {
	buf := []byte("no timestamp here at all")
	factory, ts, err := TryTimestampFormat(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)`, FmtRfc3339, buf, 3)
	if err == nil {
		t.Fatal("expected error when no timestamp present")
	}
	if factory != nil || ts != 0 {
		t.Fatalf("expected nil factory and zero ts, got %v, %d", factory, ts)
	}
}

func TestTryTimestampFormatZeroTimestamp(t *testing.T) {
	// Using epoch seconds with value "0" exercises the ts == 0 && err == nil
	// path, which should result in ErrInvalidTimestampFormat.
	buf := []byte("0")
	factory, ts, err := TryTimestampFormat(`^(0)$`, FmtEpochSeconds, buf, 1)
	if !errors.Is(err, ErrInvalidTimestampFormat) {
		t.Fatalf("expected ErrInvalidTimestampFormat, got %v", err)
	}
	if factory != nil {
		t.Fatalf("expected nil factory, got %v", factory)
	}
	if ts != 0 {
		t.Fatalf("expected zero timestamp, got %d", ts)
	}
}

func TestTryTimestampFormatGetTimestampFormatError(t *testing.T) {
	buf := []byte("anything")
	// Empty TimestampFmt triggers GetTimestampFormat error path.
	factory, ts, err := TryTimestampFormat(`.*`, TimestampFmt(""), buf, 1)
	if !errors.Is(err, ErrInvalidTimestampFormat) {
		t.Fatalf("expected ErrInvalidTimestampFormat, got %v", err)
	}
	if factory != nil || ts != 0 {
		t.Fatalf("expected nil factory and zero ts, got %v, %d", factory, ts)
	}
}

func TestGetTimestampFormatVariants(t *testing.T) {
	base := time.Date(2025, 1, 2, 3, 4, 5, 123456789, time.UTC)

	tests := []struct {
		name   string
		fmt    TimestampFmt
		layout string
	}{
		{"rfc3339nano", FmtRfc3339Nano, time.RFC3339Nano},
		{"unix", FmtUnix, time.UnixDate},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cb, err := GetTimestampFormat(tc.fmt)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got, err := cb([]byte(base.Format(tc.layout)))
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			want := base.UnixNano()
			if tc.fmt == FmtUnix {
				// UnixDate layout has second precision, so truncate expected value.
				want = base.Truncate(time.Second).UnixNano()
			}
			if got != want {
				t.Fatalf("expected %d got %d", want, got)
			}
		})
	}

	cb, err := GetTimestampFormat(TimestampFmt("custom-layout"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cb == nil {
		t.Fatal("expected non-nil callback for custom layout")
	}
}

func TestTimestampFmtString(t *testing.T) {
	tests := []struct {
		name string
		fmt  TimestampFmt
		want string
	}{
		{"rfc3339_string", FmtRfc3339, "rfc3339"},
		{"custom_string", TimestampFmt("custom-layout"), "custom-layout"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.fmt.String(); got != tc.want {
				t.Fatalf("TimestampFmt.String() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDotNotation(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{
			name:  "integer_only",
			input: "1234",
			want:  1234 * int64(time.Second),
		},
		{
			name:  "with_decimal_millis",
			input: "1234.567",
			want:  1234*int64(time.Second) + 567000000,
		},
		{
			name:  "with_decimal_micros",
			input: "1234.567890",
			want:  1234*int64(time.Second) + 567890000,
		},
		{
			name:  "with_decimal_nanos",
			input: "1234.567890123",
			want:  1234*int64(time.Second) + 567890123,
		},
		{
			name:  "higher_than_nanos_truncated",
			input: "1234.5678901234567890",
			want:  1234*int64(time.Second) + 567890123,
		},
		{
			name:  "zero_value",
			input: "0",
			want:  0,
		},
		{
			name:  "zero_with_decimal",
			input: "0.123456789",
			want:  123456789,
		},
		{
			name:  "short_fraction_padded",
			input: "1234.5",
			want:  1234*int64(time.Second) + 500000000,
		},
		{
			name:    "invalid_seconds",
			input:   "not-a-number.123",
			wantErr: true,
		},
		{
			name:    "invalid_fraction",
			input:   "1234.not-a-number",
			wantErr: true,
		},
		{
			name:    "empty_input",
			input:   "",
			wantErr: true,
		},
	}

	cb, err := GetTimestampFormat(FmtDotNotation)
	if err != nil {
		t.Fatalf("GetTimestampFormat(FmtDotNotation) failed: %v", err)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cb([]byte(tc.input))
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidTimestampFormat) {
					t.Fatalf("expected ErrInvalidTimestampFormat, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("dotNotation(%q) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}
