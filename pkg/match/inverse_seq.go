package match

import (
	"math"
	"slices"

	"github.com/prequel-dev/prequel-logmatch/pkg/entry"
	"github.com/rs/zerolog/log"
)

type InverseSeq struct {
	clock    int64
	window   int64
	gcMark   int64
	gcLeft   int64
	gcRight  int64
	nActive  int
	dupeMask bitMaskT
	terms    []termT
	resets   []resetT
}

func NewInverseSeq(window int64, seqTerms []TermT, resetTerms []ResetT) (*InverseSeq, error) {

	var (
		resets   []resetT
		nTerms   = len(seqTerms)
		terms    = make([]termT, 0, nTerms)
		dupes    = make(map[TermT]int, nTerms)
		dupeMask bitMaskT
	)

	switch {
	case nTerms > maxTerms:
		return nil, ErrTooManyTerms
	case nTerms == 0:
		return nil, ErrNoTerms
	}

	// Calculate dupes
	for _, term := range seqTerms {
		if v, ok := dupes[term]; ok {
			dupes[term] = v + 1
		} else {
			dupes[term] = 1
		}
	}

	for i, term := range seqTerms {
		m, err := term.NewMatcher()
		if err != nil {
			return nil, err
		}
		terms = append(terms, termT{matcher: m})
		if dupes[term] > 1 {
			dupeMask.Set(i)
		}
	}

	if len(resetTerms) > 0 {
		resets = make([]resetT, 0, len(resetTerms))

		for _, term := range resetTerms {
			m, err := term.Term.NewMatcher()
			switch {
			case err != nil:
				return nil, err
			case int(term.Anchor) >= len(seqTerms):
				return nil, ErrAnchorRange
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
		window:   window,
		gcLeft:   gcLeft,
		gcRight:  gcRight,
		gcMark:   disableGC,
		dupeMask: dupeMask,
		terms:    terms,
		resets:   resets,
	}, nil
}

func (r *InverseSeq) Scan(e entry.LogEntry) (hits Hits) {
	if e.Timestamp < r.clock {
		log.Warn().
			Str("line", e.Line).
			Int64("stamp", e.Timestamp).
			Int64("clock", r.clock).
			Msg("MatchSeq: Out of order event.")
		return
	}
	r.clock = e.Timestamp

	r.maybeGC(e.Timestamp)

	// Zero match optimization to avoid resets if no lookback is needed.
	var zeroMatch bool
	switch {
	case r.nActive > 0:
	case r.gcLeft > 0:
	case !r.terms[r.nActive].matcher(e.Line):
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
			return // No match on active term; NOOP.
		}

		r.terms[r.nActive].asserts = append(r.terms[r.nActive].asserts, e)
		r.nActive += 1

		r.resetGcMark(e.Timestamp + r.gcRight)

		if r.nActive < len(r.terms) {
			return
		}
	}

	return r.Eval(e.Timestamp)
}

// Assert clock, may used to close out matcher
func (r *InverseSeq) Eval(clock int64) (hits Hits) {
	var nTerms = len(r.terms)

	for r.nActive == nTerms {

		var (
			drop   = -1
			tStart = r.terms[0].asserts[0].Timestamp
			tStop  = r.terms[len(r.terms)-1].asserts[0].Timestamp
		)

		if tStop-tStart > r.window {
			drop = 0
		} else if r.resets != nil {
			retryNanos, anchor := r.checkReset(clock)

			switch {
			case anchor != math.MaxUint8:
				drop = int(anchor)
			case retryNanos > 0:
				// We have a match that is too recent; we must wait.
				return
			}
		}

		if drop >= 0 {
			// We have a negative match;
			// remove the offending term assert and continue.
			shiftLeft(r.terms, drop, 1)
		} else {
			// Fire hit and prune first assert from each term.
			hits.Cnt += 1
			if hits.Logs == nil {
				hits.Logs = make([]LogEntry, 0, nTerms)
			}

			for i, term := range r.terms {
				hits.Logs = append(hits.Logs, term.asserts[0])
				shiftLeft(r.terms, i, 1)
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

	if len(r.terms[0].asserts) == 0 {
		r.reset()
		return
	}

	type dupeT struct {
		Line      string
		Stream    string
		Timestamp int64
	}

	var (
		nActive   = 1
		dupes     map[dupeT]struct{}
		zeroMatch = r.terms[0].asserts[0].Timestamp
	)

	// Do not allocate if not processing dupes.
	// Dupe detection  is used to prune duplicate terms
	// that are incorrectly activated due to garbage collection.
	if !r.dupeMask.Zeros() {
		dupes = make(map[dupeT]struct{}, len(r.terms))
		if r.dupeMask.IsSet(0) {
			term := r.terms[0].asserts[0]
			dupes[dupeT{
				Line:      term.Line,
				Stream:    term.Stream,
				Timestamp: term.Timestamp,
			}] = struct{}{}
		}
	}

	// For remaining active terms, find the first term that is not older than the window.
	forceClear := false
	for i := 1; i < r.nActive; i++ {

		if forceClear {
			resetTerm(r.terms, i)
			continue
		}

		var cnt int

	TERMLOOP:
		for _, term := range r.terms[i].asserts {

			switch {
			case term.Timestamp < zeroMatch:
			case r.dupeMask.IsSet(i):
				dupe := dupeT{
					Line:      term.Line,
					Stream:    term.Stream,
					Timestamp: term.Timestamp,
				}
				// If term is not a dupe, we can stop.
				if _, ok := dupes[dupe]; !ok {
					break TERMLOOP
				}
			default:
				break TERMLOOP
			}
			cnt += 1
		}

		if cnt > 0 {
			shiftLeft(r.terms, i, cnt)
		}

		if len(r.terms[i].asserts) > 0 {
			nActive++

			if r.dupeMask.IsSet(i) {
				term := r.terms[i].asserts[0]
				dupes[dupeT{
					Line:      term.Line,
					Stream:    term.Stream,
					Timestamp: term.Timestamp,
				}] = struct{}{}
			}
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

func (r *InverseSeq) checkReset(clock int64) (int64, uint8) {
	// 'stamps'  escapes;  annoying.
	// TODO: consider avoiding by using s.terms[0].asserts[0].Timestamp directly
	var (
		nTerms = len(r.terms)
		stamps = make([]int64, nTerms)
	)
	for i := 0; i < nTerms; i++ {
		stamps[i] = r.terms[i].asserts[0].Timestamp
	}

	// Iterate across the resets; determine if we have a negative match.
	for i, reset := range r.resets {
		start, stop := reset.calcWindow(stamps)

		// Check if we have a negative term in the reset window.
		// TODO: Binary search?
		for _, ts := range r.resets[i].resets {
			if ts >= start && ts <= stop {
				return 0, reset.anchor
			}
		}

		// If the reset window is in the future, we cannot come to a conclusion.
		// We must wait until the reset window is in the past due to events with
		// duplicate timestamps.  Thus must wait until one tick past the reset window.
		if stop >= clock {
			return stop - clock + 1, math.MaxUint8
		}
	}

	return 0, math.MaxUint8
}

func (r *InverseSeq) resetGcMark(nMark int64) {
	if nMark < r.gcMark {
		r.gcMark = nMark
	}
}
