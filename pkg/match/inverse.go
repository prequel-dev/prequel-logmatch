package match

import (
	"errors"
	"slices"
)

var (
	ErrNoTerms       = errors.New("no terms")
	ErrTooManyTerms  = errors.New("too many terms")
	ErrAnchorRange   = errors.New("anchor out of range")
	ErrAnchorNoDupes = errors.New("non zero anchors unsupported with duplicate terms")
)

const (
	maxTerms     = 64
	capThreshold = 4
)

type ResetT struct {
	Term     TermT // Inverse term
	Window   int64 // Window size; defaults to 0 which in combination with !Absolute means the window is the range of the matched sequence.
	Slide    int64 // Slide the anchor, +/- relative to the anchor term
	Anchor   uint8 // Anchor term; defaults to first event in match sequence
	Absolute bool  // Absolute window time or relative to the range of the matched sequence.
}

type resetT struct {
	matcher  MatchFunc
	resets   []int64
	window   int64
	slide    int64
	anchor   uint8
	absolute bool
}

type termT struct {
	matcher MatchFunc
	asserts []LogEntry
}

func (r resetT) calcWindowA(anchors []anchorT) (int64, int64) {
	if len(anchors) == 0 {
		return 0, 0
	}

	var (
		width  = r.window
		anchor = anchors[r.anchor].clock
	)

	// Slide the anchor if necessary
	anchor += r.slide

	// Determine the width of the window
	if !r.absolute {
		width += anchors[len(anchors)-1].clock - anchors[0].clock
	}

	if width <= 0 {
		// Effectively disables the negative window
		width = 0
	}

	// Calculate the start and stop times
	return anchor, anchor + width
}

// Calculate GC windows for term and reset terms.

func calcGCWindow(window int64, resets []resetT) (int64, int64) {

	var (
		left  int64 = 0
		right int64 = window
	)

	// Calculate worst case scenarios here.
	// Worse case is a single term with reset window around it.
	// We need to keep the sequence around until the last possible match.
	// We also need to keep the inverse terms around in the lookback scenario.

	for _, reset := range resets {

		// Worse case scenarios:
		// relative case: window + reset.Window (positive or negative)
		// absolute case: reset.Window with last item anchor
		rRight := window + reset.window + reset.slide

		if rRight > right {
			right = rRight
		}
		if reset.slide < left {
			left = reset.slide
		}
	}

	return (-1 * left), right
}

// Be wary; this has a side effect of changing terms[i].asserts slice.

func shiftLeft(terms []termT, idx, cnt int) int {
	m := terms[idx].asserts

	switch {
	case cnt < len(m):
		m = m[cnt:]
	case cap(m) <= capThreshold:
		m = m[:0]
	default:
		m = nil
	}

	terms[idx].asserts = m
	return len(m)
}

func resetTerm(terms []termT, idx int) {
	m := terms[idx].asserts

	if cap(m) <= capThreshold {
		terms[idx].asserts = m[:0]
	} else {
		terms[idx].asserts = nil
	}
}

// Be wary; this has a side effect of changing terms[i].asserts slice.

func shiftAnchor(terms []termT, drop anchorT) int {
	if drop.offset == 0 {
		return shiftLeft(terms, drop.term, 1)
	}

	var (
		i = drop.offset
		m = terms[drop.term].asserts
	)

	m = slices.Delete(m, i, i+1)
	terms[drop.term].asserts = m
	return len(m)
}

type anchorT struct {
	clock  int64
	term   int
	offset int
}

func (a anchorT) ValidTerm() bool {
	return a.term >= 0
}
