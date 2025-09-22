package match

import (
	"testing"
)

func NewCasesSetSimple() casesT {

	return casesT{

		"IgnoreOutOfOrder": {
			// -1------ alpha
			// 2------- beta
			window: 10,
			terms:  []string{"alpha", "beta"},
			steps: []stepT{
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
			steps: []stepT{
				{line: "alpha"},
				{line: "gamma"},
				{line: "beta", cb: matchStamps(1, 3, 2)},
				{line: "gamma"},
				{line: "alpha"},
				{line: "gamma"},
				{line: "beta", cb: matchStamps(5, 7, 4), postF: checkHotMask(0b100)},
				{line: "beta", postF: checkHotMask(0b110)},
			},
		},

		"Window": {
			// A----------D------
			// --------C---------
			// -----B-------E----
			// With window of 5. should see {D,C,B}
			window: 5,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []stepT{
				{line: "alpha"},
				{line: "gamma", stamp: 4},
				{line: "beta", stamp: 7},
				{line: "alpha", stamp: 8, cb: matchStamps(8, 7, 4)},
				{line: "gamma", stamp: 9, postF: checkHotMask(0b100)},
			},
		},

		"DupeTimestamps": {
			// -A----------------
			// -B----------------
			// -C----------------
			// Dupe timestamps are tolerated.
			window: 5,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []stepT{
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
			steps: []stepT{
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
				{line: "gamma", postF: garbageCollect(50)}, // window
				{postF: checkHotMask(0b110)},
				{postF: garbageCollect(73)},
				{postF: checkHotMask(0b0)},
			},
		},

		"NOOPS": {
			window: 10,
			terms:  []string{"apple", "beta"},
			steps: []stepT{
				{postF: checkEval(12345, checkNoFire)},
				{postF: garbageCollect(12345)},
			},
		},
	}
}

func NewCasesSetDupes() casesT {

	return casesT{

		"SimpleDupes": {
			// -1-2----------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha"},
			steps: []stepT{
				{line: "alpha"},
				{line: "alpha", cb: matchStamps(1, 2)},
			},
		},

		"SimpleDupesSameTimestamp": {
			// -1-2----------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha"},
			steps: []stepT{
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
			steps: []stepT{
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
			steps: []stepT{
				{line: "alpha"},
				{line: "alpha"},
				{line: "alpha"},
				{line: "beta", cb: matchStamps(1, 2, 4)},
			},
		},

		"DupeWithOtherTerms": {
			// -1---3-------------
			// ---2---------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha", "alpha", "beta"},
			steps: []stepT{
				{line: "alpha"},
				{line: "beta"},
				{line: "alpha"},
				{line: "alpha", cb: matchStamps(1, 3, 4, 2)},
			},
		},

		"DupesObeyWindow": {
			// -1---3-------------
			// ---2---------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha", "beta"},
			steps: []stepT{
				{line: "alpha"},
				{line: "beta"},
				{line: "alpha", stamp: 7},
				{line: "alpha", stamp: 8},
				{line: "beta", stamp: 11, cb: matchStamps(7, 8, 11)},
				{line: "beta", postF: checkHotMask(0b10)},
				{line: "alpha", postF: checkHotMask(0b10)},
				{line: "beta"},
				{line: "alpha", stamp: 19},
				{line: "alpha", stamp: 19, cb: matchStamps(19, 19, 14)},
				{line: "nope", postF: checkHotMask(0b0)},
			},
		},
	}
}

func TestSets(t *testing.T) {

	cases := map[string]struct {
		cases casesT
	}{
		"Single": {
			cases: NewCasesSingle(),
		},
		"Simple": {
			cases: NewCasesSetSimple(),
		},
		"Dupes": {
			cases: NewCasesSetDupes(),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {

			tc.cases.run(t, func(tc caseT) (Matcher, error) {
				return NewMatchSet(tc.window, makeTerms(tc.terms)...)
			})

		})
	}
}

func TestSetInitFail(t *testing.T) {

	cases := map[string]struct {
		err    error
		window int64
		terms  []TermT
		reset  []ResetT
	}{
		"NoTerms": {
			err:    ErrNoTerms,
			window: 10,
		},

		"EmptyTerm": {
			err:    ErrTermEmpty,
			window: 10,
			terms:  []TermT{{Type: TermRaw, Value: ""}},
		},

		"AlmostTooManyTerms": {
			err:    nil,
			window: 10,
			terms:  makeTermsN(maxTerms),
		},

		"DupeShouldNotPushOverMax": {
			err:    nil,
			window: 10,
			terms:  makeDupesN(maxTerms * 2),
		},

		"TooManyTerms": {
			err:    ErrTooManyTerms,
			window: 10,
			terms:  makeTermsN(maxTerms + 1),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := NewMatchSet(tc.window, tc.terms...)
			if err != tc.err {
				t.Fatalf("Expected err == %v, got %v", tc.err, err)
			}
		})
	}
}
