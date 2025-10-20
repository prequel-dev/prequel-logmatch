package match

import (
	"slices"

	"github.com/prequel-dev/prequel-logmatch/pkg/entry"
	"github.com/rs/zerolog/log"
)

// InverseSeq matches a sequence of terms in order, within a time window,
// with optional reset terms that can invalidate a match.
//
// Duplicate terms are supported.  However, reset terms with non-zero anchors
// on duplicate terms are not supported.
//
// The implementation uses a state machine approach, where each term in the
// sequence is represented by a state.  As log entries are processed, the
// state machine transitions through the states based on matches.
//
// The InverseSeq struct maintains the current state of the matcher,
// including active terms, reset terms, and garbage collection markers.
//
// The Scan method processes each log entry, updating the state machine
// and checking for matches.  When a complete sequence is matched, it is
// returned as a Hit.
//
// Garbage collection is performed to remove old entries that are outside
// the time window, ensuring efficient memory usage.
//
// The Eval method can be called to force evaluation of the current state,
// and return any matches that may activate due to clock progression.
//
// Note: This implementation assumes that log entries are processed in
// chronological order. Out-of-order entries will be logged as warnings
// and ignored.

type InverseSeq struct {
	clock   int64
	window  int64
	gcMark  int64
	gcLeft  int64
	gcRight int64
	nActive int
	terms   []termT
	resets  []resetT
	dupeMap map[int]int
}

func NewInverseSeq(window int64, seqTerms []TermT, resetTerms []ResetT) (*InverseSeq, error) {

	terms, dupeMap, err := buildSeqTerms(seqTerms...)
	if err != nil {
		return nil, err
	}

	var resets []resetT
	if len(resetTerms) > 0 {
		resets = make([]resetT, 0, len(resetTerms))

		for _, term := range resetTerms {
			m, err := term.Term.NewMatcher()
			switch {
			case err != nil:
				return nil, err
			case int(term.Anchor) >= len(seqTerms):
				return nil, ErrAnchorRange
			case !maybeAnchor(len(terms), dupeMap, term.Anchor):
				return nil, ErrAnchorNoDupes
			}

			resets = append(resets, resetT{
				matcher:  m,
				window:   term.Window,
				slide:    term.Slide,
				anchor:   term.Anchor,
				absolute: term.Absolute,
			})
		}
	}

	gcLeft, gcRight := calcGCWindow(window, resets)

	return &InverseSeq{
		window:  window,
		gcLeft:  gcLeft,
		gcRight: gcRight,
		gcMark:  disableGC,
		terms:   terms,
		resets:  resets,
		dupeMap: dupeMap,
	}, nil
}

func maybeAnchor(nTerms int, dupeMap map[int]int, anchor uint8) bool {
	if dupeMap == nil || anchor == 0 {
		return true
	}

	// The anchor is non-zero so refers to a term that may be a dupe.
	// Due to the way this is implemented, we cannot support non-zero anchors on dupe terms.
	// See prequel-machine for this support.

	curOff := 0
	for i := range nTerms {
		curOff += 1

		// If anchor is on the first term before dupes, OK.
		if int(anchor) < curOff {
			return true
		}

		// If anchor is within the dupes of this term, not OK.
		curOff += dupeMap[i]
		if int(anchor) < curOff {
			return false
		}
	}

	return false
}

func (r *InverseSeq) Scan(e entry.LogEntry) (hits Hits) {
	if e.Timestamp < r.clock {
		log.Warn().
			Str("line", e.Line).
			Int64("stamp", e.Timestamp).
			Int64("clock", r.clock).
			Msg("InverseSeq: Out of order event.")
		return
	}
	r.clock = e.Timestamp

	r.maybeGC(e.Timestamp)

	// Zero match optimization if first term has no asserts yet
	var zeroMatch bool
	switch {
	case len(r.terms[0].asserts) > 0:
	case r.gcLeft > 0:
	case !r.terms[0].matcher(e.Line):
		return
	default:
		zeroMatch = true
	}

	// Run resets
	for i, reset := range r.resets {
		if reset.matcher(e.Line) {
			r.resets[i].resets = append(reset.resets, e.Timestamp)
			r.resetGcMark(e.Timestamp + r.gcLeft + r.gcRight)
		}
	}

	// Run the active terms
	for i := range r.nActive {
		if r.terms[i].matcher(e.Line) {
			r.terms[i].asserts = append(r.terms[i].asserts, e)
		}
	}

	if r.nActive < len(r.terms) {

		switch {
		case zeroMatch:
		case !r.terms[r.nActive].matcher(e.Line):
			// No match on active term; NOOP.
			return
		}

		r.terms[r.nActive].asserts = append(r.terms[r.nActive].asserts, e)
		r.resetGcMark(e.Timestamp + r.gcRight)

		// We have matched the active term; check if there are dupes before advancing.
		dupeCnt := r.dupeMap[r.nActive]

		if len(r.terms[r.nActive].asserts) <= dupeCnt {
			// Not enough dupes yet; append current for later.
			return
		}

		// We've matched the active term; advance.
		r.nActive += 1

		if r.nActive < len(r.terms) {
			return
		}
	}

	return r._eval(e.Timestamp)
}

func (r *InverseSeq) Eval(clock int64) (hits Hits) {
	// If clock is less than or equal to current clock, do nothing.
	// In those cases we've already processed up to the current clock.
	if clock <= r.clock {
		return
	}
	r.clock = clock
	return r._eval(clock)
}

func (r *InverseSeq) _eval(clock int64) (hits Hits) {
	nTerms := len(r.terms)

	for r.nActive == nTerms {

		var (
			drop    = anchorT{term: -1}
			dupeCnt = r.dupeMap[nTerms-1]
			tStart  = r.terms[0].asserts[0].Timestamp
			tStop   = r.terms[nTerms-1].asserts[dupeCnt].Timestamp
		)

		if tStop-tStart > r.window {
			drop.term = 0
		} else if r.resets != nil {
			anchor := r.checkReset(clock)

			switch {
			case anchor.ValidTerm():
				drop = anchor
			case anchor.clock > 0:
				// We have a match that is too recent; we must wait.
				return
			}
		}

		if drop.ValidTerm() {
			// We have a negative match;
			// remove the offending term assert and continue.
			shiftAnchor(r.terms, drop)
		} else {
			// Fire hit and prune asserts
			hits.Cnt += 1
			if hits.Logs == nil {
				hits.Logs = make([]LogEntry, 0, nTerms+r.dupeMap[-1])
			}

			for i, term := range r.terms {
				hitCnt := r.dupeMap[i] + 1
				hits.Logs = append(hits.Logs, term.asserts[:hitCnt]...)

				// Remove all used asserts
				shiftLeft(r.terms, i, hitCnt)
			}

		}

		// Fixup state
		r.miniGC()
	}

	return
}

func (r *InverseSeq) maybeGC(clock int64) {

	if clock < r.gcMark {
		return
	}

	r.GarbageCollect(clock)
}

// Remove all terms that are older than the window.
func (r *InverseSeq) GarbageCollect(clock int64) {

	// Special case;
	// If all the terms are hot and we have resets,
	// allow the GC to be handled on the next evaluation.
	// Otherwise, we may GC an valid single term prematurely.
	if r.nActive == len(r.terms) && len(r.resets) > 0 {
		r.gcMark = disableGC
		return
	}

	var (
		cnt      int
		nMark    = disableGC
		m        = r.terms[0].asserts
		deadline = clock - r.gcRight
	)

	// Find the first term that is not older than the window.
	// Binary search?
	for _, term := range m {
		if term.Timestamp >= deadline {
			break
		}
		cnt += 1
	}

	if cnt > 0 {
		shiftLeft(r.terms, 0, cnt)
	}

	r.miniGC()

	if r.nActive > 0 {
		nMark = r.terms[0].asserts[0].Timestamp + r.gcRight
	}

	// Adjust the deadline for the reset terms
	deadline -= r.gcLeft

	// Clean up the reset terms
	for i, reset := range r.resets {

		var (
			m = reset.resets
		)

		if len(m) == 0 {
			continue
		}

		cnt, _ := slices.BinarySearch(m, deadline)

		if cnt > 0 {
			r.resets[i].resets = m[cnt:]
		}

		if len(r.resets[i].resets) > 0 {
			v := r.resets[i].resets[0] + r.gcLeft + r.gcRight
			if v < nMark {
				nMark = v
			}
		}
	}

	r.gcMark = nMark
}

// Find the first term in the sequence.
// Remove each term older than that, it cannot be in sequence.
// Update the nActive count.

func (r *InverseSeq) miniGC() {

	nZeroAsserts := len(r.terms[0].asserts)
	if nZeroAsserts == 0 {
		r.reset()
		return
	}

	var (
		nActive    int
		dupeCnt    = r.dupeMap[0]
		zeroMatch  = r.terms[0].asserts[0].Timestamp
		forceClear bool
	)

	if nZeroAsserts > dupeCnt {
		nActive = 1
	} else {
		forceClear = true
	}

	// For remaining active terms, find the first term that is not older than the window.
	for i := 1; i < r.nActive; i++ {

		if forceClear {
			resetTerm(r.terms, i)
			continue
		}

		cnt := 0
		for _, t := range r.terms[i].asserts {
			if t.Timestamp < zeroMatch {
				cnt += 1
			} else {
				zeroMatch = t.Timestamp
				break
			}
		}

		if cnt > 0 {
			shiftLeft(r.terms, i, cnt)
		}

		dupeCnt := r.dupeMap[i]
		if len(r.terms[i].asserts) > dupeCnt {
			nActive++
		} else {
			forceClear = true
		}
	}

	r.nActive = nActive

}

func (r *InverseSeq) reset() {
	for i := range r.terms {
		resetTerm(r.terms, i)
	}
	r.nActive = 0
}

func (r *InverseSeq) checkReset(clock int64) anchorT {

	var (
		nTerms  = len(r.terms)
		nDupes  = r.dupeMap[-1]
		anchors = make([]anchorT, 0, nTerms+nDupes)
	)

	// Gather timestamps from match
	for i, term := range r.terms {
		cnt := r.dupeMap[i] + 1
		for j := range cnt {
			anchors = append(anchors, anchorT{
				clock:  term.asserts[j].Timestamp,
				term:   i,
				offset: j,
			})
		}
	}

	// Iterate across the resets; determine if we have a negative match.
	for i, reset := range r.resets {
		start, stop := reset.calcWindowA(anchors)

		// Check if we have a negative term in the reset window.
		// TODO: Binary search?
		for _, ts := range r.resets[i].resets {
			if ts >= start && ts <= stop {
				return anchors[reset.anchor]
			}
		}

		// If the reset window is in the future, we cannot come to a conclusion.
		// We must wait until the reset window is in the past due to events with
		// duplicate timestamps.  Thus must wait until one tick past the reset window.
		if stop >= clock {
			return anchorT{
				term:  -1,
				clock: stop - clock + 1,
			}
		}
	}

	return anchorT{term: -1}
}

func (r *InverseSeq) resetGcMark(nMark int64) {
	if nMark < r.gcMark {
		r.gcMark = nMark
	}
}
