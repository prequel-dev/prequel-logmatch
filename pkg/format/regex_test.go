package format

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestRegex(t *testing.T) {

	exp := `^((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s\d{2}:\d{2}:\d{2}\s\d{4}) `
	factory, err := NewRegexFactory(exp, WithTimeFormat(time.ANSIC))
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	f := factory.New()

	line := []byte(`Mon Jan  9 15:04:05 2020 Funky log line indeed.`)

	entry, err := f.ReadEntry(line)
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	if entry.Timestamp != 1578582245000000000 {
		t.Errorf("Expected %d got %d", 1578582245000000000, entry.Timestamp)
	}

	if entry.Line != string(line) {
		t.Errorf("Expected %s got %s", string(line), entry.Line)
	}
}

func TestRegexReadTimestampFail(t *testing.T) {

	exp := `^(\d{2}) ([A-Za-z]{3}) (\d{2}) (\d{2}:\d{2}) ([+-]\d{4})`
	factory, err := NewRegexFactory(exp, WithTimeFormat(time.RFC822Z))
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	f := factory.New()

	line := []byte(`10 Jan 12 15:04 -0700 Testy stamp.`)
	_, err = f.ReadTimestamp(bytes.NewReader(line))

	if err == nil {
		t.Errorf("Expected error got nil")
	}
}

func TestRegexReadTimestamp(t *testing.T) {

	exp := `(\d{2}\s[A-Za-z]{3}\s\d{2}\s\d{2}:\d{2}\s[-+]\d{4})`
	factory, err := NewRegexFactory(exp, WithTimeFormat(time.RFC822Z))
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	f := factory.New()

	line := []byte(`10 Jan 12 15:04 -0700 Testy stamp.`)
	ts, err := f.ReadTimestamp(bytes.NewReader(line))

	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	if ts != 1326233040000000000 {
		t.Errorf("Expected %d got %d", 1326233040000000000, ts)
	}
}

func TestRegexCustomCb(t *testing.T) {

	exp := `"time":(\d{18,19})`
	cb := func(m []byte) (int64, error) {
		nanoEpoch, err := strconv.ParseInt(string(m), 10, 64)
		if err == nil {
			return nanoEpoch, nil
		}

		return 0, fmt.Errorf("expected int64")
	}

	line := []byte(`{"some_similar_field":1618070400000000001,"time":1618070400000000000,"message":"test"}`)
	factory, err := NewRegexFactory(exp, cb)
	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	f := factory.New()
	ts, err := f.ReadTimestamp(bytes.NewReader(line))

	if err != nil {
		t.Errorf("Expected nil error got %v", err)
	}

	if ts != 1618070400000000000 {
		t.Errorf("Expected %d got %d", 1618070400000000000, ts)
	}
}

func TestMungeYearWithSlop(t *testing.T) {
	t.Run("PanicOnInvalidDuration", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic but did not get one")
			}
		}()
		_ = mungeYearWithSlop(time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC), time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC), -time.Hour)
	})

	tests := []struct {
		name       string
		now        time.Time
		tsTime     time.Time
		futureSlop time.Duration
		expected   time.Time
	}{
		// Before now
		{
			name:       "BeforeNow",
			now:        time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 1, 1, 10, 0, 0, 0, time.UTC),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		},
		{
			name:       "ExactlyNow",
			now:        time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 1, 2, 12, 0, 0, 0, time.UTC),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC),
		},
		{
			name:       "WithinSlop",
			now:        time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 1, 8, 10, 0, 0, 0, time.UTC),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2026, 1, 8, 10, 0, 0, 0, time.UTC),
		},
		{
			name:       "AtSlopBoundary",
			now:        time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 1, 9, 12, 0, 0, 0, time.UTC),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC),
		},
		{
			name:       "JustAfterSlop",
			now:        time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 1, 10, 10, 0, 0, 0, time.UTC),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2025, 1, 10, 10, 0, 0, 0, time.UTC),
		},
		{
			name:       "NewYearJustBefore",
			now:        time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 12, 20, 10, 0, 0, 0, time.UTC),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2026, 12, 20, 10, 0, 0, 0, time.UTC),
		},
		{
			name:       "NewYearJustBeforeSlop",
			now:        time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 1, 6, 10, 0, 0, 0, time.UTC),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2027, 1, 6, 10, 0, 0, 0, time.UTC),
		},
		{
			name:       "NewYearJustAfterSlop",
			now:        time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 1, 8, 10, 0, 0, 0, time.UTC),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2026, 1, 8, 10, 0, 0, 0, time.UTC),
		},
		{
			name:       "SlightPastSlop",
			now:        time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 1, 1, 12, 0, 0, 0, time.UTC),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		},
		{
			name:       "SlightPastExactlySlop",
			now:        time.Date(2026, 1, 8, 12, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 1, 1, 12, 0, 0, 0, time.UTC),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		},
		{
			name:       "SlightPastBeyondSlopThisYear",
			now:        time.Date(2026, 1, 9, 12, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 1, 1, 12, 0, 0, 0, time.UTC),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		},
		{
			name:       "SlightPastWithinSlopLastYear",
			now:        time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 12, 31, 12, 0, 0, 0, time.UTC),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2025, 12, 31, 12, 0, 0, 0, time.UTC),
		},
		{
			name:       "SlightPastBeyondSlopLastYear",
			now:        time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 12, 31, 12, 0, 0, 0, time.UTC),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2025, 12, 31, 12, 0, 0, 0, time.UTC),
		},
		{
			name:       "this year normal",
			now:        time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 6, 10, 10, 0, 0, 0, time.UTC),
			futureSlop: 7 * 24 * time.Hour,
			expected:   time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC),
		},
		{
			name:       "future within slop",
			now:        time.Date(2026, 12, 28, 12, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 12, 30, 10, 0, 0, 0, time.UTC),
			futureSlop: 5 * 24 * time.Hour,
			expected:   time.Date(2026, 12, 30, 10, 0, 0, 0, time.UTC),
		},
		{
			name:       "future beyond slop",
			now:        time.Date(2026, 12, 28, 12, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 1, 10, 10, 0, 0, 0, time.UTC),
			futureSlop: 5 * 24 * time.Hour,
			expected:   time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC),
		},
		{
			name:       "dec to jan positive slop",
			now:        time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 1, 6, 10, 0, 0, 0, time.UTC),
			futureSlop: 7 * 24 * time.Hour,
			expected:   time.Date(2027, 1, 6, 10, 0, 0, 0, time.UTC),
		},
		{
			name:       "jan to dec positive slop",
			now:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			tsTime:     time.Date(0, 12, 31, 23, 0, 0, 0, time.UTC),
			futureSlop: 7 * 24 * time.Hour,
			expected:   time.Date(2025, 12, 31, 23, 0, 0, 0, time.UTC),
		},
		{
			name:       "TimezoneHandling",
			now:        time.Date(2026, 1, 2, 12, 0, 0, 0, time.FixedZone("EST", -5*3600)),
			tsTime:     time.Date(0, 1, 2, 12, 0, 0, 0, time.FixedZone("EST", -5*3600)),
			futureSlop: time.Hour * 24 * 7,
			expected:   time.Date(2026, 1, 2, 17, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				got      = mungeYearWithSlop(tt.now, tt.tsTime, tt.futureSlop)
				expected = tt.expected.UnixNano()
			)
			if got != expected {
				t.Errorf("mungeYear() = (%d)%v, want (%d)%v", got, time.Unix(0, got).UTC(), expected, tt.expected)
			}
		})
	}
}
