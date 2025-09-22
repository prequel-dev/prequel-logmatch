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
	terms   []termT
	hotMask bitMaskT
	dupeMap map[int]int
}

func NewMatchSet(window int64, setTerms ...TermT) (*MatchSet, error) {

	terms, dupeMap, err := buildSetTerms(setTerms...)
	if err != nil {
		return nil, err
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

			if dupeCnt := r.dupeMap[i]; len(r.terms[i].asserts) > dupeCnt {
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

		var (
			dupeCnt = r.dupeMap[i]
			hitCnt  = 1 + dupeCnt
		)

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
			if len(m) <= dupeCnt {
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
			m = r.terms[i].asserts
		)

		if len(m) == 0 {
			r.hotMask.Clr(i)
		} else {
			if dupeCnt := r.dupeMap[i]; len(m) <= dupeCnt {
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

func buildSetTerms(setTerms ...TermT) ([]termT, map[int]int, error) {

	if len(setTerms) == 0 {
		return nil, nil, ErrNoTerms
	}

	var (
		i       int
		nTerms  = len(setTerms)
		dupeMap map[int]int
		uniqs   = make(map[TermT]int, nTerms)
		terms   = make([]termT, 0, nTerms)
	)

	// O(n) on nTerms
	for _, term := range setTerms {

		if idx, ok := uniqs[term]; ok {
			if dupeMap == nil {
				dupeMap = make(map[int]int)
			}
			dupeMap[idx]++
		} else {
			m, err := term.NewMatcher()
			if err != nil {
				return nil, nil, err
			}
			terms = append(terms, termT{matcher: m})
			uniqs[term] = i
			i += 1
		}
	}

	if len(terms) > maxTerms {
		return nil, nil, ErrTooManyTerms
	}

	// Check if over allocated due to dupes
	if cap(terms) > len(terms) {
		nTerms := make([]termT, len(terms))
		copy(nTerms, terms)
		terms = nTerms
	}

	return terms, dupeMap, nil
}
