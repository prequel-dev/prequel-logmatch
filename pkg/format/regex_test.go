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

func TestMungeYear(t *testing.T) {

	var (
		baseTime = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)
		baseNow  = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	)

	tests := map[string]struct {
		delta time.Duration
		want  time.Time
	}{
		"simple": {
			delta: 0,
		},
		"slightly in the future": {
			delta: time.Hour * 24,
		},
		"slightly in the past": {
			delta: -time.Hour * 24,
		},
		"4 weeks in the past": {
			delta: -time.Hour * 24 * 7,
		},
		"36 weeks in the past": {
			delta: -time.Hour * 24 * 7 * 36,
		},
		"44 weeks in the past interpreted as the future": {
			delta: -time.Hour * 24 * 7 * 44,
			want:  time.Date(2020, 2, 27, 0, 0, 0, 0, time.UTC),
		},
		"4 weeks in the future": {
			delta: time.Hour * 24 * 7 * 4,
		},
		"8 weeks in the future": {
			delta: time.Hour * 24 * 7 * 8,
		},
		"8 weeks and 4 days in the future minus 1 hour": {
			delta: time.Hour*24*7*8 + 4*time.Hour*24 - time.Hour,
			want:  time.Date(2020, 2, 29, 23, 0, 0, 0, time.UTC),
		},
		"8 weeks and 4 days in the future": {
			delta: time.Hour*24*7*8 + 4*time.Hour*24,
			want:  time.Date(2019, 3, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {

			src := baseTime.Add(tc.delta)
			got := mungeYear(baseNow, src)

			if src.Year() > 0 {
				t.Errorf("Expected %d got %d", 0, src.Year())
			}

			ts := time.Unix(0, got).UTC()

			if tc.want.IsZero() {
				tc.want = baseNow.Add(tc.delta)
			}

			if !ts.Equal(tc.want) {
				t.Errorf("Expected %s got %s", tc.want, ts)
			}
		})
	}
}
