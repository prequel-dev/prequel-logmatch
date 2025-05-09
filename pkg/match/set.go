package match

import (
	"math"

	"github.com/rs/zerolog/log"
)

const disableGC int64 = math.MaxInt64

type MatchSet struct {
	clock   int64
	window  int64
	gcMark  int64
	hotMask bitMaskT
	terms   []termT
	dupeMap map[int]int
}

func NewMatchSet(window int64, setTerms ...TermT) (*MatchSet, error) {

	var (
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

	return &MatchSet{
		terms:   terms,
		window:  window,
		gcMark:  disableGC,
		dupeMap: dupeMap, // 8 bytes overhead if nil, same as a bitmask
	}, nil
}

func (r *MatchSet) Scan(e LogEntry) (hits Hits) {
	if e.Timestamp < r.clock {
		log.Warn().
			Str("line", e.Line).
			Int64("stamp", e.Timestamp).
			Int64("clock", r.clock).
			Msg("MatchSet: Out of order event.")
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

			// Update the gcMark if the timestamp is less than the current gcMark
			if e.Timestamp < r.gcMark {
				r.gcMark = e.Timestamp
			}
		}
	}

	if !r.hotMask.FirstN(len(r.terms)) {
		return // no match
	}

	// We have a full frame; fire and prune.
	hits.Cnt = 1
	hits.Logs = make([]LogEntry, 0, len(r.terms)) // Not quite if dupes are present

	r.gcMark = disableGC
	for i, term := range r.terms {

		hitCnt := 1
		dupeCnt := r.dupeMap[i]
		if dupeCnt > 0 {
			hitCnt = dupeCnt
		}

		m := term.asserts
		hits.Logs = append(hits.Logs, m[0:hitCnt]...)
		if len(m) == hitCnt && cap(m) <= capThreshold {
			m = m[:0]
		} else {
			m = m[hitCnt:]
		}
		r.terms[i].asserts = m

		if len(m) == 0 {
			r.hotMask.Clr(i)
		} else {
			// Clear the hot mask if there's a dupeCnt and we're under it
			if len(m) < dupeCnt {
				r.hotMask.Clr(i)
			}

			// Update the gcMark if earliest timestamp is less than the current gcMark
			if v := m[0].Timestamp; v < r.gcMark {
				r.gcMark = v
			}
		}
	}

	return
}

func (r *MatchSet) maybeGC(clock int64) {
	if (r.hotMask.Zeros() && r.dupeMap == nil) || clock-r.gcMark <= r.window {
		return
	}

	r.GarbageCollect(clock)
}

// Remove all terms that are older than the window.
func (r *MatchSet) GarbageCollect(clock int64) {

	deadline := clock - r.window

	r.gcMark = disableGC

	for i, term := range r.terms {

		var cnt int

		for _, assert := range term.asserts {
			if assert.Timestamp >= deadline {
				break
			}
			cnt += 1
		}

		if cnt > 0 {
			shiftLeft(r.terms, i, cnt)
		}

		var (
			m       = r.terms[i].asserts
			dupeCnt = r.dupeMap[i]
		)

		if len(m) == 0 {
			r.hotMask.Clr(i)
		} else {
			if len(m) < dupeCnt {
				r.hotMask.Clr(i)
			}
			if v := m[0].Timestamp; v < r.gcMark {
				r.gcMark = v
			}
		}

	}
}

// Because match sequence is edge triggered, there won't be hits.  But can GC.
func (r *MatchSet) Eval(clock int64) (h Hits) {
	return
}
