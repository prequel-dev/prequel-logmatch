package match

import (
	"time"

	"github.com/prequel-dev/prequel-logmatch/pkg/entry"
)

type InverseAlone struct {
	active   int64
	window   int64
	matchers []MatchFunc
}

// Simplistic inverse matching for a set of terms within a time window.
// The window is open while none of the terms match; and resets if any term matches.
// The machine will fire once every time the time window boundary threshold is crossed.
// It will fire:
// - As a result of a Scan containing a LogEntry that causes the boundry threshold cross.
// - As a result of a Poll() that uses real time to check if boundary threshold cross.
// 		(The Poll() option only works accurately if the data is real time)

func NewInverseAlone(window time.Duration, terms ...string) (*InverseAlone, error) {

	var (
		nTerms   = len(terms)
		matchers = make([]MatchFunc, nTerms)
	)

	for i, term := range terms {
		if m, err := makeMatchFunc(term); err != nil {
			return nil, err
		} else {
			matchers[i] = m
		}
	}

	return &InverseAlone{
		window:   int64(window),
		matchers: matchers,
	}, nil

}

func (r *InverseAlone) Scan(e LogEntry) (hits Hits) {

	if r.active > 0 {
		if e.Timestamp-r.active >= r.window {
			// We hit the window boundary; fire.
			hits.Cnt = 1
			hits.Logs = append(hits.Logs, LogEntry{Timestamp: r.active})
			r.active = e.Timestamp
		}
	} else {
		r.active = e.Timestamp // First entry.
	}

LOOP:
	for _, m := range r.matchers {
		if m(e.Line) {
			r.active = 0
			break LOOP
		}
	}

	return
}

// Poll for state change.
// Returns HIT if currently active.
// Should poll again in 'dur'.
// Poll doesn't work consistently on historic data, has to be a live feed.

func (r *InverseAlone) Poll() (hits Hits, hint time.Duration) {
	hint = time.Duration(r.window)
	if r.active == 0 {
		return
	}

	now := time.Now().UnixNano()

	if now-r.active >= r.window {
		hits.Cnt = 1
		hits.Logs = append(hits.Logs, LogEntry{Timestamp: r.active})
		r.active = now
	} else {
		hint = time.Duration(now - r.active)
	}

	return
}

func (r *InverseAlone) State() []byte {
	return nil
}

type InverseSeq struct {
	mseq     *MatchSeq
	active   int64
	window   int64
	matchers []MatchFunc
	pending  Hits
}

// Hybrid of alone inverse match with sequence matching.  The inverseTerms
// must stay active for the time.Duration window or the machine is reset.
// Fire rules are different than inverse alone.
// - Will fire any pending sequence matches on boundary cross.
// - Will fire any subsequent sequence matches while active after boundary cross.
// - Will also fire on Poll to handle case where sequence match occurs before
//   activation, but no subsequent event scans to edge trigger boundary cross.
//
// Not quite a general solution; cannot create a sequence that describes inverse
// terms between matches on regular terms.

func NewInverseSeq(window time.Duration, inverseTerms, seqTerms []string) (*InverseSeq, error) {

	var (
		nTerms   = len(inverseTerms)
		matchers = make([]MatchFunc, nTerms)
	)

	for i, term := range inverseTerms {
		if m, err := makeMatchFunc(term); err != nil {
			return nil, err
		} else {
			matchers[i] = m
		}
	}

	mseq, err := NewMatchSeq(window, seqTerms...)
	if err != nil {
		return nil, err
	}

	return &InverseSeq{
		mseq:     mseq,
		window:   int64(window),
		matchers: matchers,
	}, nil
}

func (r *InverseSeq) Scan(e entry.LogEntry) (hits Hits) {

	if r.active > 0 {
		if r.pending.Cnt > 0 && e.Timestamp-r.active >= r.window {
			// We hit the window boundary; fire.
			hits = r.pending
			r.pending.Cnt = 0
			r.pending.Logs = nil
		}

	} else {
		r.active = e.Timestamp // First entry
	}

	for _, m := range r.matchers {
		if m(e.Line) {
			r.mseq.reset()
			r.active = 0
			r.pending.Cnt = 0
			r.pending.Logs = nil
			return
		}
	}

	// If we got this far, then run we are still active.
	// Run the sequence matchers.
	nHits := r.mseq.Scan(e)

	switch {
	case nHits.Cnt <= 0:
		// No sequence match; just keep going.
	case e.Timestamp-r.active >= r.window:
		// We are in an active state; fire immediately.
		hits.Cnt += nHits.Cnt
		hits.Logs = append(hits.Logs, nHits.Logs...)
	default:
		// These are pending
		r.pending.Cnt += nHits.Cnt
		r.pending.Logs = append(r.pending.Logs, nHits.Logs...)
	}

	return
}

// Poll for state change.
// Returns HIT if currently active.
// Should poll again in 'dur'.
// Poll doesn't work consistently on historic data, has to be a live feed.

func (r *InverseSeq) Poll() (hits Hits, hint time.Duration) {
	hint = time.Duration(r.window)
	if r.active == 0 {
		return
	}

	now := time.Now().UnixNano()

	if now-r.active >= r.window {
		hits = r.pending
		r.pending.Cnt = 0
		r.pending.Logs = nil
	} else {
		hint = time.Duration(now - r.active)
	}

	return
}

func (r *InverseSeq) State() []byte {
	return nil
}
