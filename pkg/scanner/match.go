package scanner

import (
	"cmp"
	"regexp"
	"slices"

	"github.com/rs/zerolog/log"
)

type MatchFlagT int

type ModeT int

const (
	ModeEnrich ModeT = iota
	ModeJump
	ModeFilter
	ModeInvert
)

type ExprT struct {
	RegEx *regexp.Regexp
	Mode  ModeT
}

const (
	MatchForceUTF16 MatchFlagT = 1 << iota
)

type mfuncT func(line string) ([][]int, bool)

type MatchScan struct {
	sz    int
	maxSz int
	clip  bool
	logs  []LogEntry

	flags MatchFlagT
	exprs []ExprT
}

func NewMatchScan(maxSz int, flags MatchFlagT, exprs []ExprT) *MatchScan {

	return &MatchScan{
		maxSz: maxSz,
		logs:  make([]LogEntry, 0, maxSz/avgLogSize),
		flags: flags,
		exprs: exprs,
	}
}

func (sr *MatchScan) Bind() ScanFuncT {

	var (
		forceUtf16 = sr.flags&MatchForceUTF16 != 0
		mfuncs     = normalize(sr.exprs)
	)

	return func(entry LogEntry) bool {

		for _, m := range mfuncs {
			hits, ok := m(entry.Line)
			switch {
			case !ok:
				return false
			case hits != nil:
				if forceUtf16 {
					hits = hitsToUtf16(entry.Line, hits)
				}
				entry.Matches = append(entry.Matches, hits...)
			}
		}

		sz := entry.Size()
		if sr.sz += sz; sr.sz > sr.maxSz {
			sr.clip = true
			sr.sz -= sz
			return true
		}

		sr.logs = append(sr.logs, entry)
		return false
	}
}

func (sr *MatchScan) Result() ScanResultT {
	return ScanResultT{
		Sz:   sr.sz,
		Clip: sr.clip,
		Logs: sr.logs,
	}
}

func makeInvert(exp *regexp.Regexp) mfuncT {
	return func(line string) ([][]int, bool) {
		return nil, !exp.MatchString(line)
	}
}

func makeFilter(exp *regexp.Regexp) mfuncT {
	return func(line string) ([][]int, bool) {
		return nil, exp.MatchString(line)
	}
}

func makeFilterEnrich(exp *regexp.Regexp) mfuncT {
	return func(line string) ([][]int, bool) {
		hits := exp.FindAllStringIndex(line, -1)
		return hits, hits != nil
	}
}

func makeJump(exp *regexp.Regexp) mfuncT {
	found := false
	return func(line string) ([][]int, bool) {
		if found {
			return nil, true
		}
		found = exp.MatchString(line)
		return nil, found
	}
}

func makeJumpEnrich(exp *regexp.Regexp) mfuncT {
	found := false
	return func(line string) ([][]int, bool) {
		if hits := exp.FindAllStringIndex(line, -1); hits != nil {
			found = true
			return hits, true
		}

		return nil, found
	}
}

func makeEnrich(exp *regexp.Regexp) mfuncT {
	return func(line string) ([][]int, bool) {
		return exp.FindAllStringIndex(line, -1), true
	}
}

func makeFunc(exp ExprT, enrich bool) (mfunc mfuncT) {

	switch exp.Mode {
	case ModeEnrich:
		mfunc = makeEnrich(exp.RegEx)
	case ModeInvert:
		mfunc = makeInvert(exp.RegEx)
	case ModeJump:
		if enrich {
			mfunc = makeJumpEnrich(exp.RegEx)
		} else {
			mfunc = makeJump(exp.RegEx)
		}
	case ModeFilter:
		if enrich {
			mfunc = makeFilterEnrich(exp.RegEx)
		} else {
			mfunc = makeFilter(exp.RegEx)
		}

	default:
		panic("unknown match mode")
	}

	return
}

// Remove all duplicate expressions.
// Execute in known order.

func normalize(exprs []ExprT) []mfuncT {

	m := make(map[string][]ExprT, len(exprs))

	for _, expr := range exprs {
		s := expr.RegEx.String()
		m[s] = append(m[s], expr)
	}

	dexprs := make([]dexpT, 0, len(exprs))

	for _, v := range m {
		dexprs = append(dexprs, dedupe(v)...)
	}

	// Run in known order; [Invert,Filter,Jump,Enrich]
	// (Happends to be reverse enum.)
	slices.SortFunc(dexprs, func(a, b dexpT) int {
		return -1 * cmp.Compare(a.exp.Mode, b.exp.Mode)
	})

	// Convert the qualified filter list into functions
	mFuncs := make([]mfuncT, 0, len(dexprs))
	for _, v := range dexprs {
		log.Trace().
			Bool("enrich", v.enrich).
			Str("regex", v.exp.RegEx.String()).
			Int("mode", int(v.exp.Mode)).
			Msg("Add function")
		mFuncs = append(mFuncs, makeFunc(v.exp, v.enrich))
	}

	return mFuncs
}

// Certain functions with identical regex can be combined as an optimization

type dexpT struct {
	exp    ExprT
	enrich bool
}

func dedupe(exprs []ExprT) []dexpT {

	if len(exprs) == 1 {
		return []dexpT{
			{exp: exprs[0]},
		}
	}

	// O(n) First scan for mode duplicates; should be unusual.
	var (
		bmap  int64
		nList = make([]dexpT, 0, len(exprs))
	)

	for _, v := range exprs {
		if v.Mode > 64 {
			log.Warn().Int("mode", int(v.Mode)).Msg("Corrupted mode; ignore.")
		} else if bmap&(1<<int(v.Mode)) == 0 {
			nList = append(nList, dexpT{exp: v})
			bmap |= (1 << int(v.Mode))
		} else {
			// We have a dupe. Skip it.
			log.Debug().
				Int("mode", int(v.Mode)).
				Str("regex", v.RegEx.String()).
				Msg("Skip dupe regex expression")
		}
	}

	// Optimize known states; avoid running mulitple regex expressions we could run just one.
	var (
		noChange bool
		nEnrich  bool
		nMode    ModeT
	)

	switch bmap {
	case 3: // Enrich and Jump -> jump_enrich
		nMode = ModeJump
		nEnrich = true

	case 5: // Enrich and filter -> filter_enrich
		nMode = ModeFilter
		nEnrich = true

	case 6: // Filter and jump -> filter
		nMode = ModeFilter

	case 7: // Enrich, jump, filter -> filter_enrich
		nMode = ModeFilter
		nEnrich = true

	case 9: // Enrich, invert -> Invert (no matches)
		nMode = ModeInvert

	default:
		noChange = true
	}

	if !noChange {
		nList = []dexpT{{
			exp: ExprT{
				RegEx: nList[0].exp.RegEx,
				Mode:  nMode,
			},
			enrich: nEnrich,
		}}
	}

	return nList
}
