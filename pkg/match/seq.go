package match

import (
	"github.com/rs/zerolog/log"
)

// MatchSeq implements a simplistic state machine where transaction from one
// state (slot) to the next is a successful match.  When machine reaches final state
// (ie. all slots active), a match is emitted.
//
// The state machine will reset if the initial matching slot ages out of the time window.
// The machine is edge triggered, state can only change on a new event.  As such,
// it works properly when scanning a log that is not aligned with real time.
//
// Note: The matcher does not currently enforce strict ordering on match.  This means
// that if two matches in a sequence have the same timestamp, it will be considered a match.
// This is done to account for imprecise clocks; a clock with low resolution might emit
// two events with the same timestamp when in real time they are sequential.

type MatchSeq struct {
	clock   int64
	window  int64
	nActive int
	terms   []termT
	dupeMap map[int]int
}

func NewMatchSeq(window int64, seqTerms ...TermT) (*MatchSeq, error) {

	terms, dupeMap, err := buildSeqTerms(seqTerms...)
	if err != nil {
		return nil, err
	}

	return &MatchSeq{
		window:  window,
		terms:   terms,
		dupeMap: dupeMap,
	}, nil
}

func (r *MatchSeq) Scan(e LogEntry) (hits Hits) {

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

	for i := range r.nActive {
		if r.terms[i].matcher(e.Line) {
			r.terms[i].asserts = append(r.terms[i].asserts, e)
		}
	}

	if !r.terms[r.nActive].matcher(e.Line) {
		// No match on active term; NOOP.
		return
	}

	// We have matched the active term; check if there are dupes before advancing.
	dupeCnt := r.dupeMap[r.nActive]

	if len(r.terms[r.nActive].asserts) < dupeCnt {
		// Not enough dupes yet; append current for later.
		r.terms[r.nActive].asserts = append(r.terms[r.nActive].asserts, e)
		return
	}

	// We matched the active term, but not the all terms yet.
	// Advance the active term and append the current event.
	if r.nActive+1 < len(r.terms) {
		r.terms[r.nActive].asserts = append(r.terms[r.nActive].asserts, e)
		r.nActive += 1
		return
	}

	// We have a full frame; fire and prune.
	hits.Cnt = 1
	hits.Logs = make([]LogEntry, 0, len(r.terms)+r.dupeMap[-1])

	for i := range len(r.terms) - 1 {
		hitCnt := r.dupeMap[i] + 1
		hits.Logs = append(hits.Logs, r.terms[i].asserts[:hitCnt]...)

		// Only remove the first item; leave remaining dupes for next match.
		shiftLeft(r.terms, i, 1)
	}

	// Append any dupes for the final term
	if dupeCnt > 0 {
		hits.Logs = append(hits.Logs, r.terms[r.nActive].asserts[0:dupeCnt]...)

		// Only remove the first item; leave remaining dupes for next match.
		shiftLeft(r.terms, r.nActive, 1)
	}

	// And the final event that triggered this hit
	hits.Logs = append(hits.Logs, e)

	// Update active so the miniGC can cleanup up correctly
	r.nActive += 1

	// Fixup state
	r.miniGC()

	return
}

func (r *MatchSeq) maybeGC(clock int64) {
	if r.nActive == 0 || clock-r.terms[0].asserts[0].Timestamp < r.window {
		return
	}

	r.GarbageCollect(clock)
}

// Remove all terms that are older than the window.
func (r *MatchSeq) GarbageCollect(clock int64) {
	var (
		cnt      int
		m        = r.terms[0].asserts
		deadline = clock - r.window
	)

	// Find the first term that is not older than the window.
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
}

func (r *MatchSeq) miniGC() {

	if len(r.terms[0].asserts) == 0 {
		r.reset()
		return
	}

	var (
		nActive     = 0
		forceClear  bool
		zeroMatch   int64
		zeroAsserts = r.terms[0].asserts
		zeroDupes   = r.dupeMap[0]
	)

	if len(zeroAsserts) < zeroDupes+1 {
		forceClear = true
	} else {
		zeroMatch = zeroAsserts[zeroDupes].Timestamp
		nActive += 1
	}

	// For remaining active terms, find the first term that is not older than the window.
	for i := 1; i < r.nActive; i++ {

		if forceClear {
			resetTerm(r.terms, i)
			continue
		}

		var (
			cnt int
			m   = r.terms[i].asserts
		)
	TERMLOOP:
		for _, term := range m {
			switch {
			case term.Timestamp < zeroMatch:
			default:
				break TERMLOOP
			}
			cnt += 1
		}

		if cnt > 0 {
			shiftLeft(r.terms, i, cnt)
		}

		if len(r.terms[i].asserts) > r.dupeMap[i] {
			nActive++
		} else {
			forceClear = true
		}
	}

	r.nActive = nActive
}

func (r *MatchSeq) reset() {
	for i := range r.terms {
		m := r.terms[i].asserts
		if cap(m) <= capThreshold {
			r.terms[i].asserts = m[:0]
		} else {
			r.terms[i].asserts = nil
		}
	}
	r.nActive = 0
}

// Because match sequence is edge triggered, there won't be hits.
func (r *MatchSeq) Eval(clock int64) (h Hits) {
	return
}

func buildSeqTerms(seqTerms ...TermT) ([]termT, map[int]int, error) {

	if len(seqTerms) == 0 {
		return nil, nil, ErrNoTerms
	}

	var (
		i        = -1
		lastTerm TermT
		dupeMap  map[int]int
		dupeSum  int
		nTerms   = len(seqTerms)
		terms    = make([]termT, 0, nTerms)
	)

	for _, term := range seqTerms {

		switch {
		case i == -1: // First time
			fallthrough
		case term != lastTerm:
			m, err := term.NewMatcher()
			if err != nil {
				return nil, nil, err
			}
			i += 1
			terms = append(terms, termT{matcher: m})
			lastTerm = term
		case dupeMap == nil: // dupe
			dupeMap = make(map[int]int)
			fallthrough
		default:
			dupeMap[i]++
			dupeSum++
		}
	}

	if len(terms) > maxTerms {
		return nil, nil, ErrTooManyTerms
	}

	// Stuff dupeSum in dupeMap as an optimization
	if dupeSum > 0 {
		dupeMap[-1] = dupeSum
	}

	// Check if over allocated due to dupes
	if cap(terms) > len(terms) {
		nTerms := make([]termT, len(terms))
		copy(nTerms, terms)
		terms = nTerms
	}

	return terms, dupeMap, nil
}
