package munge

import "time"

const defaultMungeSlop = time.Hour * 24 * 7

// Munge the year to be the closest to now, but not more than 'defaultMungeSlop' in the future.
func MungeYear(now, t time.Time) int64 {
	return MungeYearWithSlop(now, t, defaultMungeSlop)
}

// mungeYearWithSlop assigns a year to a timestamp `t` that has no year specified.
// It chooses the closest year to `now`, allowing the resulting time to be up to `futureSlop`
// in the future relative to `now`.
//
// Behavior:
//   - Interpret `t` using the same month, day, and time, but choose a year that makes it
//     as close as possible to `now`, while not placing it more than `futureSlop` into the future.
//   - If choosing the current year would put `t` more than `futureSlop` ahead of `now`,
//     the year is adjusted (e.g. to the previous or next year) to satisfy this constraint.
//   - A negative `futureSlop` will cause a panic.
//
// Example:
//
//	now        = Dec 31, 2026 23:00
//	t          = Jan 6 10:00        // no year specified
//	futureSlop = 7*24*time.Hour
//	→ returns Jan 6, 2027 10:00
func MungeYearWithSlop(now, t time.Time, futureSlop time.Duration) int64 {
	if futureSlop < 0 {
		panic("futureSlop cannot be negative")
	}

	// Compute the reference time including slop
	nowWithSlop := now.Add(futureSlop).UTC()

	// Force UTC
	t = t.UTC()

	// Build candidate timestamp using the year of nowWithSlop
	candidate := time.Date(
		nowWithSlop.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		time.UTC,
	)

	// Adjust candidate if it falls outside the allowed window
	// Positive slop: timestamps too far in the future belong to last year
	if candidate.After(nowWithSlop) {
		candidate = candidate.AddDate(-1, 0, 0)
	}

	return candidate.UTC().UnixNano()
}
