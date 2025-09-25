package match

import (
	"fmt"
	"testing"

	"github.com/prequel-dev/prequel-logmatch/pkg/entry"
)

type stepT struct {
	stamp int64
	line  string
	cb    func(*testing.T, int, Hits)
	postF func(*testing.T, int, Matcher)
}

type caseT struct {
	clock  int64
	window int64
	terms  []string
	reset  []ResetT
	steps  []stepT
}

type casesT map[string]caseT

func (c casesT) run(t *testing.T, factory func(caseT) (Matcher, error)) {
	t.Helper()

	for name, tc := range c {
		t.Run(name, func(t *testing.T) {
			t.Helper()

			sm, err := factory(tc)
			if err != nil {
				t.Fatalf("Expected err == nil, got %v", err)
			}

			clock := tc.clock

			for idx, step := range tc.steps {

				clock += 1
				stamp := clock
				if step.stamp != 0 {
					stamp = step.stamp
					clock = stamp
				}

				if step.line != "" {
					var (
						entry = entry.LogEntry{Timestamp: stamp, Line: step.line}
						hits  = sm.Scan(entry)
					)

					if step.cb == nil {
						checkNoFire(t, idx+1, hits)
					} else {
						step.cb(t, idx+1, hits)
					}
				}

				if step.postF != nil {
					step.postF(t, idx+1, sm)
				}
			}
		})
	}
}

func matchStamps(stamps ...int64) func(*testing.T, int, Hits) {
	return matchStampsN(1, stamps...)
}

func matchStampsN(cnt int, stamps ...int64) func(*testing.T, int, Hits) {
	return func(t *testing.T, step int, hits Hits) {
		t.Helper()
		if cnt != hits.Cnt {
			t.Errorf("Step %v: Expected %v hits, got %v", step, cnt, hits.Cnt)
			return
		}

		for i, stamp := range stamps {
			if hits.Logs[i].Timestamp != stamp {
				t.Errorf("Step %v: Expected %v, got %v on index %v", step, stamp, hits.Logs[i].Timestamp, i)
			}
		}
	}
}

func matchLines(lines ...string) func(*testing.T, int, Hits) {
	return matchLinesN(1, lines...)
}

func matchLinesN(cnt int, lines ...string) func(*testing.T, int, Hits) {
	return func(t *testing.T, step int, hits Hits) {
		t.Helper()
		if cnt != hits.Cnt {
			t.Errorf("Step %v: Expected %v hits, got %v", step, cnt, hits.Cnt)
			return
		}

		for i, line := range lines {
			if hits.Logs[i].Line != line {
				t.Errorf("Step %v: Expected %v, got %v on index %v", step, line, hits.Logs[i].Line, i)
			}
		}
	}
}

func checkActive(nActive int) func(*testing.T, int, Matcher) {
	return func(t *testing.T, step int, sm Matcher) {
		t.Helper()
		var active int
		switch v := any(sm).(type) {
		case *MatchSeq:
			active = v.nActive
		case *InverseSeq:
			active = v.nActive
		default:
			panic("Invalid type")
		}

		if active != nActive {
			t.Errorf("Step %v: Expected nActive == %v, got %v", step, active, nActive)
		}
	}
}

func checkHotMask(mask int64) func(*testing.T, int, Matcher) {
	return func(t *testing.T, step int, sm Matcher) {
		t.Helper()
		var hotMask bitMaskT
		switch v := any(sm).(type) {
		case *InverseSet:
			hotMask = v.hotMask
		case *MatchSet:
			hotMask = v.hotMask
		default:
			panic("Invalid type")
		}

		if hotMask != bitMaskT(mask) {
			t.Errorf("Step %v: Expected hotMask == %b, got %b", step, mask, hotMask)
		}
	}
}

func checkNoFire(t *testing.T, step int, hits Hits) {
	t.Helper()
	if hits.Cnt != 0 {
		t.Errorf("Step %v: Expected 0 hits, got %v", step, hits.Cnt)
	}
}

func checkResets(idx int, cnt int) func(*testing.T, int, Matcher) {
	return func(t *testing.T, step int, sm Matcher) {
		t.Helper()
		var resetCnt int
		switch v := any(sm).(type) {
		case *InverseSet:
			resetCnt = len(v.resets[idx].resets)
		case *InverseSeq:
			resetCnt = len(v.resets[idx].resets)
		default:
			panic("Invalid type")
		}

		if resetCnt != cnt {
			t.Errorf(
				"Step %v: Expected %v resets on idx: %v, got %v",
				step,
				cnt,
				idx,
				resetCnt,
			)
		}
	}
}

func checkGCMark(mark int64) func(*testing.T, int, Matcher) {
	return func(t *testing.T, step int, sm Matcher) {
		t.Helper()
		var gcMark int64
		switch v := any(sm).(type) {
		case *InverseSet:
			gcMark = v.gcMark
		case *InverseSeq:
			gcMark = v.gcMark
		default:
			panic("Invalid type")
		}

		if gcMark != mark {
			t.Errorf("Step %v: Expected gcMark == %v, got %v", step, mark, gcMark)
		}
	}
}

func checkEval(clock int64, cb func(*testing.T, int, Hits)) func(*testing.T, int, Matcher) {
	return func(t *testing.T, step int, sm Matcher) {
		t.Helper()
		hits := sm.Eval(clock)
		cb(t, step, hits)
	}
}

func garbageCollect(clock int64) func(*testing.T, int, Matcher) {
	return func(t *testing.T, step int, sm Matcher) {
		t.Helper()
		sm.GarbageCollect(clock)
	}
}

func makeTerms(terms []string) []TermT {
	out := make([]TermT, 0, len(terms))
	for _, term := range terms {
		out = append(out, TermT{Type: TermRaw, Value: term})
	}
	return out
}
func makeTermsA(terms ...string) []TermT {
	return makeTerms(terms)
}

func makeRaw(term string) TermT {
	return TermT{Type: TermRaw, Value: term}
}

func makeTermsN(n int) []TermT {
	terms := make([]TermT, n)
	for i := range n {
		terms[i] = makeRaw(fmt.Sprintf("term %d", i))
	}
	return terms
}

func makeDupesN(n int) []TermT {
	var out []TermT
	for range n {
		out = append(out, TermT{Type: TermRaw, Value: "dupe"})
	}
	return out
}

func TestCalcWindow(t *testing.T) {
	var anchors []anchorT

	r := resetT{}
	r.calcWindowA(anchors)
}
