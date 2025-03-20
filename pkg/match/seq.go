package match

import (
	"github.com/rs/zerolog/log"
)

// MatchSeq implements a simplistic state machine where transaction from one
// state (slot) to the next is a succesful match.  When machine reaches final state
// (ie. all slots active), a match is emitted.
//
// The state machine will reset if the intial matching slot ages out of the time window.
// The machine is edge triggered, state can only change on a new event.  As such,
// it works properly when scanning a log that is not aligned with real time.
//
// Note: The matcher does not currently enforce strict ordering on match.  This means
// that if two matches in a sequence have the same timestamp, it will be considered a match.
// This is done to account for imprecise clocks; a clock with low resolution might emit
// two events with the same timestamp when in fact they are not.

type MatchSeq struct {
	clock   int64
	window  int64
	nActive int
	terms   []termT
}

func NewMatchSeq(window int64, terms ...string) (*MatchSeq, error) {
	var (
		nTerms = len(terms)
		termL  = make([]termT, nTerms)
	)

	for i, term := range terms {
		if m, err := makeMatchFunc(term); err != nil {
			return nil, err
		} else {
			termL[i].matcher = m
		}
	}

	return &MatchSeq{
		window: window,
		terms:  termL,
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

	// We matched the active term
	r.nActive += 1

	if r.nActive < len(r.terms) {
		// Not all terms are matched; append current for later.
		r.terms[r.nActive-1].asserts = append(r.terms[r.nActive-1].asserts, e)
		return
	}

	// We have a full frame; fire and prune.
	hits.Cnt = 1
	hits.Logs = make([]LogEntry, 0, len(r.terms))

	for i := range len(r.terms) - 1 {
		hits.Logs = append(hits.Logs, r.terms[i].asserts[0])
		shiftLeft(r.terms, i, 1)
	}

	// And the final event that triggered this hit
	hits.Logs = append(hits.Logs, e)

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
		nActive   = 1
		zeroMatch = r.terms[0].asserts[0].Timestamp
	)

	// For remaining active terms, find the first term that is not older than the window.
	for i := 1; i < r.nActive; i++ {

		var (
			cnt int
			m   = r.terms[i].asserts
		)
		for _, term := range m {
			if term.Timestamp >= zeroMatch {
				break
			}
			cnt += 1
		}

		if cnt > 0 {
			shiftLeft(r.terms, i, cnt)
		}

		if len(r.terms[i].asserts) > 0 {
			nActive++
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
