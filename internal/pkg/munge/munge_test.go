package munge

import (
	"testing"
	"time"
)

func TestMungeYearWithSlop(t *testing.T) {
	t.Run("PanicOnInvalidDuration", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic but did not get one")
			}
		}()
		_ = MungeYearWithSlop(time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC), time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC), -time.Hour)
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
				got      = MungeYearWithSlop(tt.now, tt.tsTime, tt.futureSlop)
				expected = tt.expected.UnixNano()
			)
			if got != expected {
				t.Errorf("MungeYearWithSlop() = (%d)%v, want (%d)%v", got, time.Unix(0, got).UTC(), expected, tt.expected)
			}
		})
	}
}
