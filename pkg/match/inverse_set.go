package match

import (
	"cmp"
	"math"
	"slices"

	"github.com/prequel-dev/prequel-logmatch/pkg/entry"
	"github.com/rs/zerolog/log"
)

type InverseSet struct {
	clock   int64
	window  int64
	gcMark  int64
	gcLeft  int64
	gcRight int64
	hotMask bitMaskT
	terms   []termT
	resets  []resetT
	dupeMap map[int]int
}

func NewInverseSet(window int64, setTerms []TermT, resetTerms []ResetT) (*InverseSet, error) {

	var (
		resets  []resetT
		dupeMap map[int]int
		nTerms  = len(setTerms)
		dupes   = make(map[TermT]int, nTerms)
		terms   = make([]termT, 0, nTerms)
	)

	switch {
	case nTerms > maxTerms:
		return nil, ErrTooManyTerms
	case nTerms == 0:
		return nil, ErrNoTerms
	}

	// First pass to get term counts
	for _, term := range setTerms {
		dupes[term]++
	}

	// Iterate over the terms again to build the matcher list
	for i, term := range setTerms {

		cnt := dupes[term]

		if cnt >= 1 {

			m, err := term.NewMatcher()
			if err != nil {
				return nil, err
			}

			terms = append(terms, termT{matcher: m})

			if cnt > 1 {

				// We have a dupe; add it to the dupeMap
				if dupeMap == nil {
					dupeMap = make(map[int]int)
				}
				dupeMap[i] = cnt

				// Delete term from the map to prevent adding it again
				delete(dupes, term)
			}
		}
	}

	if len(resetTerms) > 0 {
		resets = make([]resetT, 0, len(resetTerms))

		for _, term := range resetTerms {
			m, err := term.Term.NewMatcher()
			switch {
			case err != nil:
				return nil, err
			case int(term.Anchor) >= len(setTerms):
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

	return &InverseSet{
		window:  window,
		gcLeft:  gcLeft,
		gcRight: gcRight,
		gcMark:  disableGC,
		terms:   terms,
		resets:  resets,
		dupeMap: dupeMap,
	}, nil
}

func (r *InverseSet) Scan(e entry.LogEntry) (hits Hits) {
	if e.Timestamp < r.clock {
		log.Warn().
			Str("line", e.Line).
			Int64("stamp", e.Timestamp).
			Int64("clock", r.clock).
			Msg("InverseSet: Out of order event.")
		return
	}
	r.clock = e.Timestamp

	r.maybeGC(e.Timestamp)

	// For a set, must scan all terms.
	// Cannot short circuit like a sequence.
	for i, term := range r.terms {
		if term.matcher(e.Line) {
			// Append the match to the assert list
			r.terms[i].asserts = append(r.terms[i].asserts, e)

			// If not a dupe or we've hit the dupe count, set the hot mask
			if dupeCnt, ok := r.dupeMap[i]; !ok || len(r.terms[i].asserts) >= dupeCnt {
				r.hotMask.Set(i)
			}

			// Update the gcMark given the timestamp of the match
			r.resetGcMark(e.Timestamp + r.gcRight)
		}
	}

	if r.hotMask.Zeros() && r.gcLeft == 0 {
		// Nothing HOT and no point running resets.
		return
	}

	// Run resets
	for i, reset := range r.resets {
		if reset.matcher(e.Line) {
			r.resets[i].resets = append(reset.resets, e.Timestamp)
			r.resetGcMark(e.Timestamp + r.gcLeft + r.gcRight)
		}
	}

	if !r.hotMask.FirstN(len(r.terms)) {
		return // no match
	}

	return r.Eval(e.Timestamp)
}

// Assert clock, may used to close out matcher
func (r *InverseSet) Eval(clock int64) (hits Hits) {
	var nTerms = len(r.terms)

	for r.hotMask.FirstN(nTerms) {

		drop := anchorT{term: -1}

		// Cannot depend on GC to determine whether we are still in the window.
		// This is because we might have an extended GC due to a long reset window.
		mIdx, tStart, tStop := r.frameMatch()

		if tStop-tStart > r.window {
			drop.term = mIdx
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
			if dupeCnt := r.dupeMap[drop.term]; dupeCnt <= 0 {
				if shiftLeft(r.terms, drop.term, 1) == 0 {
					r.hotMask.Clr(drop.term)
				}
			} else if cnt := shiftAnchor(r.terms, drop); cnt < dupeCnt {
				r.hotMask.Clr(drop.term)
			}

		} else {
			// Fire hit and prune first assert from each term.
			hits.Cnt += 1
			if hits.Logs == nil {
				hits.Logs = make([]LogEntry, 0, nTerms)
			}

			for i, term := range r.terms {
				cnt := 1
				if dupeCnt, ok := r.dupeMap[i]; ok {
					cnt = dupeCnt
				}
				hits.Logs = append(hits.Logs, term.asserts[0:cnt]...)
				if shiftLeft(r.terms, i, cnt) < cnt {
					r.hotMask.Clr(i)
				}
			}
		}
	}

	return
}

type anchorT struct {
	clock  int64
	term   int
	offset int
}

func (a anchorT) ValidTerm() bool {
	return a.term >= 0
}

func (r *InverseSet) checkReset(clock int64) anchorT {

	var (
		nTerms  = len(r.terms)
		anchors = make([]anchorT, 0, nTerms) //nTerms not right when dupes is used.
	)

	// Gather timestamps from match
	for i, term := range r.terms {
		cnt := 1
		if dupeCnt, ok := r.dupeMap[i]; ok {
			cnt = dupeCnt
		}
		for j := range cnt {
			anchors = append(anchors, anchorT{
				clock:  term.asserts[j].Timestamp,
				term:   i,
				offset: j,
			})
		}
	}

	// Sort the anchors so that the anchors are relative to the sorted sequence.
	// If we do not sort, the anchor is relative to the original term, which
	// may be desirable, but is not the usual intent for an anchor.
	slices.SortFunc(anchors, func(a, b anchorT) int {
		return cmp.Compare(a.clock, b.clock)
	})

	// Filter the anchor list to just stamps
	// About 20 extra nanoseconds and an extra allocation.
	stamps := make([]int64, len(anchors))
	for i, anchor := range anchors {
		stamps[i] = anchor.clock
	}

	// Iterate across the resets; determine if we have a negative match.
	for _, reset := range r.resets {
		start, stop := reset.calcWindow(stamps)

		// Check if we have a negative term in the reset window.
		// TODO: Binary search?
		for _, ts := range reset.resets {
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

// Assumes we are hot; determine the start, stop time of the match.
// Return the anchor term as well.

func (r *InverseSet) frameMatch() (int, int64, int64) {

	var (
		minAnchor int
		tStart    int64 = math.MaxInt64
		tStop     int64
	)

	// Fast path no dupes
	if len(r.dupeMap) == 0 {
		// O(n) on terms
		for i, term := range r.terms {
			stamp := term.asserts[0].Timestamp
			if stamp < tStart {
				tStart = stamp
				minAnchor = i
			}
			if stamp > tStop {
				tStop = stamp
			}
		}

	} else {
		// O(n) on terms
		for i, term := range r.terms {

			cnt := 1
			if dupeCnt, ok := r.dupeMap[i]; ok {
				cnt = dupeCnt
			}

			// Find the minimum timestamp of the term
			for j := range cnt {
				stamp := term.asserts[j].Timestamp
				if stamp < tStart {
					tStart = stamp
					minAnchor = i
				}
				if stamp > tStop {
					tStop = stamp
				}
			}
		}
	}

	return minAnchor, tStart, tStop
}

func (r *InverseSet) maybeGC(clock int64) {

	if clock < r.gcMark {
		return
	}

	r.GarbageCollect(clock)
}

// Remove all terms that are older than the window.
func (r *InverseSet) GarbageCollect(clock int64) {

	// Special case;
	// If all the terms are hot and we have resets,
	// allow the GC to be handled on the next evaluation.
	// Otherwise, we may GC a valid single term prematurely.
	if len(r.resets) > 0 && r.hotMask.FirstN(len(r.terms)) {
		r.gcMark = disableGC
		return
	}

	var (
		nMark    = disableGC
		deadline = clock - r.gcRight
	)

	for i, term := range r.terms {

		var (
			cnt int
		)

		// Find the first term that is not older than the window.
		// Binary search?
		for _, term := range term.asserts {
			if term.Timestamp >= deadline {
				break
			}
			cnt += 1
		}

		if cnt > 0 {
			if shiftLeft(r.terms, i, cnt) == 0 {
				r.hotMask.Clr(i)
			}
		}

		if len(r.terms[i].asserts) > 0 {
			if v := r.terms[i].asserts[0].Timestamp + r.gcRight; v < nMark {
				nMark = v
			}
		}
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

func (r *InverseSet) resetGcMark(nMark int64) {
	if nMark < r.gcMark {
		r.gcMark = nMark
	}
}
