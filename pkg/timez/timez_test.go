package timez

import (
	"errors"
	"math"
	"math/big"
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
		{"zero", "0", 0},
		{"seconds", "1000", 1000 * int64(time.Second)},
		{"millis", "12345678901", 12345678901 * int64(time.Millisecond)},
		{"micros", "12345678901234", 12345678901234 * int64(time.Microsecond)},
		{"nanos", "1234567890123456789", 1234567890123456789},
		{"maxInt", "9223372036854775807", 9223372036854775807},
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

	// overflow
	if _, err := cb([]byte("9223372036854775808")); !errors.Is(err, ErrInvalidTimestampFormat) {
		t.Fatalf("expected ErrInvalidTimestampFormat for overflow, got %v", err)
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

func TestToUnixNano(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected int64
	}{
		// Seconds
		{"seconds epoch", 0, 0},
		{"seconds 1", 1, 1_000_000_000},
		{"seconds 1234567890", 1234567890, 1234567890_000_000_000},
		{"seconds negative", -1, -1_000_000_000},
		{"seconds negative min", -maxSecondsToInt64, -maxSecondsToInt64 * multiplesOfSecondsToInt64},
		{"seconds max", maxSecondsToInt64, maxSecondsToInt64 * multiplesOfSecondsToInt64},

		// Milliseconds
		{"milliseconds min", maxSecondsToInt64 + 1, (maxSecondsToInt64 + 1) * multiplesOfMillisecondsToInt64},
		{"milliseconds negative max", -(maxSecondsToInt64 + 1), -(maxSecondsToInt64 + 1) * multiplesOfMillisecondsToInt64},
		{"milliseconds 100_000_000_000", 100_000_000_000, 100_000_000_000 * multiplesOfMillisecondsToInt64},
		{"milliseconds negative", -100_000_000_000, -100_000_000_000 * multiplesOfMillisecondsToInt64},
		{"milliseconds max", maxMillisecondsToInt64, maxMillisecondsToInt64 * multiplesOfMillisecondsToInt64},

		// Microseconds
		{"microseconds min", maxMillisecondsToInt64 + 1, (maxMillisecondsToInt64 + 1) * multiplesOfMicrosecondsToInt64},
		{"microseconds negative max", -(maxMillisecondsToInt64 + 1), -(maxMillisecondsToInt64 + 1) * multiplesOfMicrosecondsToInt64},
		{"microseconds 100_000_000_000_000", 100_000_000_000_000, 100_000_000_000_000 * multiplesOfMicrosecondsToInt64},
		{"microseconds negative", -100_000_000_000_000, -100_000_000_000_000 * multiplesOfMicrosecondsToInt64},
		{"microseconds max", maxMicrosecondsToInt64, maxMicrosecondsToInt64 * multiplesOfMicrosecondsToInt64},

		// Nanoseconds
		{"nanoseconds min", maxMicrosecondsToInt64 + 1, (maxMicrosecondsToInt64 + 1)},
		{"nanoseconds negative max", -(maxMicrosecondsToInt64 + 1), -(maxMicrosecondsToInt64 + 1)},
		{"nanoseconds 100_000_000_000_000_000", 100_000_000_000_000_000, 100_000_000_000_000_000},
		{"nanoseconds negative", -100_000_000_000_000_000, -100_000_000_000_000_000},

		// Edge cases
		{"max int64 - 1", math.MaxInt64 - 1, math.MaxInt64 - 1},
		{"min int64 + 1", math.MinInt64 + 1, math.MinInt64 + 1},
		{"max int64", math.MaxInt64, math.MaxInt64},
		{"min int64", math.MinInt64, math.MinInt64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToUnixNano(tt.input)
			if result != tt.expected {
				t.Errorf("ToUnixNano(%d) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestMaxToInt64MultiplicationOverflow(t *testing.T) {

	// Validate that the constants and multipliers used in ToUnixNano do not cause overflow when multiplied,
	// which would break the heuristic logic.  Demonstrate that the values are on the edge of the limits
	// by incrementing past the limits and validating that the overflow occurs as expected.

	type test struct {
		name       string
		value      int64
		multiplier int64
		overflow   bool
	}
	tests := []test{
		{"seconds", maxSecondsToInt64, multiplesOfSecondsToInt64, false},
		{"seconds negative", -maxSecondsToInt64, multiplesOfSecondsToInt64, false},
		{"milliseconds", maxMillisecondsToInt64, multiplesOfMillisecondsToInt64, false},
		{"milliseconds negative", -maxMillisecondsToInt64, multiplesOfMillisecondsToInt64, false},
		{"microseconds", maxMicrosecondsToInt64, multiplesOfMicrosecondsToInt64, false},
		{"microseconds negative", -maxMicrosecondsToInt64, multiplesOfMicrosecondsToInt64, false},
		{"seconds_overflow", maxSecondsToInt64 + 1, multiplesOfSecondsToInt64, true},
		{"seconds_negative_overflow", -(maxSecondsToInt64 + 1), multiplesOfSecondsToInt64, true},
		{"milliseconds_overflow", maxMillisecondsToInt64 + 1, multiplesOfMillisecondsToInt64, true},
		{"milliseconds_negative_overflow", -(maxMillisecondsToInt64 + 1), multiplesOfMillisecondsToInt64, true},
		{"microseconds_overflow", maxMicrosecondsToInt64 + 1, multiplesOfMicrosecondsToInt64, true},
		{"microseconds_negative_overflow", -(maxMicrosecondsToInt64 + 1), multiplesOfMicrosecondsToInt64, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use big.Int to check for overflow
			var (
				bigResult = new(big.Int).Mul(big.NewInt(tt.value), big.NewInt(tt.multiplier))
				fits      = bigResult.IsInt64()
			)

			switch {
			case !tt.overflow && fits:
				// expected to fit and does fit, test passes
			case !tt.overflow && !fits:
				t.Errorf("Expected no overflow for %s: %d * %d = %s does not fit in int64, but should fit", tt.name, tt.value, tt.multiplier, bigResult.String())
			case tt.overflow && fits:
				t.Errorf("Expected overflow for %s: %d * %d = %s fits in int64, but should overflow", tt.name, tt.value, tt.multiplier, bigResult.String())
			default:
				// expected to overflow and does overflow, test passes
			}
		})
	}
}
