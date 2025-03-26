package match

import (
	"github.com/prequel-dev/prequel-logmatch/pkg/entry"
)

type MatchSingle struct {
	matcher MatchFunc
}

func NewMatchSingle(term TermT) (*MatchSingle, error) {
	m, err := term.NewMatcher()
	if err != nil {
		return nil, err
	}

	return &MatchSingle{matcher: m}, nil
}

func (r *MatchSingle) Scan(e entry.LogEntry) (hits Hits) {

	if r.matcher(e.Line) {
		hits.Cnt = 1
		hits.Logs = []entry.LogEntry{e}
	}

	return
}

func (r *MatchSingle) Eval(clock int64) (hits Hits) {
	return
}

func (r *MatchSingle) GarbageCollect(clock int64) {
}
