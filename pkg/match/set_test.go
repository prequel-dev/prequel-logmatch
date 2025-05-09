package match

import (
	"fmt"
	"testing"

	"github.com/prequel-dev/prequel-logmatch/pkg/entry"
)

func TestSet(t *testing.T) {

	type step = stepT[MatchSet]

	var tests = map[string]struct {
		clock  int64
		window int64
		terms  []string
		steps  []step
	}{
		"SingleTerm": {
			// -A---------------- alpha
			window: 10,
			terms:  []string{"alpha"},
			steps: []step{
				{line: "alpha", cb: matchStamps(1)},
			},
		},

		"IgnoreOutOfOrder": {
			// -1------ alpha
			// 2------- beta
			window: 10,
			terms:  []string{"alpha", "beta"},
			steps: []step{
				{line: "alpha", stamp: 2},
				{line: "beta", stamp: 1},
			},
		},

		"Simple": {
			// A--------E-------
			// -----C-------G-H--
			// --B-----D--F------
			// Should see {A,C,B} {E,G,D}
			window: 50,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []step{
				{line: "alpha"},
				{line: "gamma"},
				{line: "beta", cb: matchStamps(1, 3, 2)},
				{line: "gamma"},
				{line: "alpha"},
				{line: "gamma"},
				{line: "beta", cb: matchStamps(5, 7, 4), postF: checkHotMask[MatchSet](0b100)},
				{line: "beta", postF: checkHotMask[MatchSet](0b110)},
			},
		},

		"Window": {
			// A----------D------
			// --------C---------
			// -----B-------E----
			// With window of 5. should see {D,C,B}
			window: 5,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []step{
				{line: "alpha"},
				{line: "gamma", stamp: 4},
				{line: "beta", stamp: 7},
				{line: "alpha", stamp: 8, cb: matchStamps(8, 7, 4)},
				{line: "gamma", stamp: 9, postF: checkHotMask[MatchSet](0b100)},
			},
		},

		"DupeTimestamps": {
			// -A----------------
			// -B----------------
			// -C----------------
			// Dupe timestamps are tolerated.
			window: 5,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []step{
				{line: "alpha", stamp: 1},
				{line: "gamma", stamp: 1},
				{line: "beta", stamp: 1, cb: matchStamps(1, 1, 1)},
			},
		},

		"GarbageCollectOldTerms": {
			// -1------4--------------10----------
			// ---2--3----------8---9-----11----
			// ----------5--6-7---------------12-
			// Should fire {1,2,5}, {4,3,6}, {10,8,7}
			window: 50,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []step{
				{line: "alpha"},
				{line: "beta"},
				{line: "beta"},
				{line: "alpha"},
				{line: "gamma", cb: matchStamps(1, 2, 5)},
				{line: "gamma", cb: matchStamps(4, 3, 6)},
				{line: "gamma"},
				{line: "beta"},
				{line: "beta"},
				{line: "alpha", cb: matchStamps(10, 8, 7)},
				{line: "beta"},
				{line: "gamma", postF: garbageCollect[*MatchSet](50)}, // window
				{postF: checkHotMask[MatchSet](0b110)},
				{postF: garbageCollect[*MatchSet](73)},
				{postF: checkHotMask[MatchSet](0b0)},
			},
		},

		"SimpleDupes": {
			// -1-2----------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha"},
			steps: []step{
				{line: "alpha"},
				{line: "alpha", cb: matchStamps(1, 2)},
			},
		},

		"SimpleDupesSameTimestamp": {
			// -1-2----------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha"},
			steps: []step{
				{line: "alpha", stamp: 1},
				{line: "alpha", stamp: 1, cb: matchStamps(1, 1)},
			},
		},

		"DupesWithOtherTerms": {
			// -1---3-------------
			// ---2---------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha", "beta"},
			steps: []step{
				{line: "alpha"},
				{line: "beta"},
				{line: "alpha", cb: matchStamps(1, 3, 2)},
			},
		},

		"DupesWithOtherTermsAndExtras": {
			// -1---3-------------
			// ---2---------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha", "beta"},
			steps: []step{
				{line: "alpha"},
				{line: "alpha"},
				{line: "alpha"},
				{line: "beta", cb: matchStamps(1, 2, 4)},
			},
		},

		"DupesObeyWindow": {
			// -1---3-------------
			// ---2---------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha", "beta"},
			steps: []step{
				{line: "alpha"},
				{line: "beta"},
				{line: "alpha", stamp: 7},
				{line: "alpha", stamp: 8},
				{line: "beta", stamp: 11, cb: matchStamps(7, 8, 11)},
				{line: "beta", postF: checkHotMask[MatchSet](0b10)},
				{line: "alpha", postF: checkHotMask[MatchSet](0b10)},
				{line: "beta"},
				{line: "alpha", stamp: 19},
				{line: "alpha", stamp: 19, cb: matchStamps(19, 19, 14)},
				{line: "nope", postF: checkHotMask[MatchSet](0b0)},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			sm, err := NewMatchSet(tc.window, makeTerms(tc.terms)...)
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

func TestSetNoTerms(t *testing.T) {
	_, err := NewMatchSet(10)
	if err != ErrNoTerms {
		t.Fatalf("Expected err == ErrNoTerms, got %v", err)
	}
}

func TestSetTooManyTerms(t *testing.T) {

	terms := make([]TermT, maxTerms)
	for i := range maxTerms {
		terms[i] = makeRaw(fmt.Sprintf("term %d", i))
	}
	_, err := NewMatchSet(10, terms...)
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	terms = append(terms, makeRaw("one too many"))

	_, err = NewMatchSet(10, terms...)
	if err != ErrTooManyTerms {
		t.Fatalf("Expected err == ErrTooManyTerms, got %v", err)
	}
}

func TestSetEmptyTerm(t *testing.T) {
	term := TermT{Type: TermRaw, Value: ""}
	_, err := NewMatchSet(10, term)
	if err != ErrTermEmpty {
		t.Fatalf("Expected err == ErrTermEmpty, got %v", err)
	}
}
