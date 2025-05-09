package match

import (
	"fmt"
	"testing"
	"time"

	"github.com/prequel-dev/prequel-logmatch/pkg/entry"
	"github.com/rs/zerolog"
)

func TestSetInverse(t *testing.T) {
	type step = stepT[InverseSet]

	var tests = map[string]struct {
		clock  int64
		window int64
		terms  []string
		reset  []ResetT
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

		"SingleTermResetHit": {
			// -A---------------- alpha
			// ------------------ reset
			terms: []string{"alpha"},
			reset: []ResetT{
				{
					Window: 10,
					Term:   makeRaw("reset"),
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "NOOP", stamp: 10},                      // fire slightly early
				{line: "reset", stamp: 12, cb: matchStamps(1)}, // Fire reset late
			},
		},

		"SingleTermResetMiss": {
			// -A---------------- alpha
			// -----------B------ reset
			terms: []string{"alpha"},
			reset: []ResetT{
				{
					Window: 10,
					Term:   makeRaw("reset"),
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "reset", stamp: 11},
				{line: "NOOP", stamp: 1000},
			},
		},

		"SingleTermDupeTimestampReset": {
			//	-1---------------- alpha
			//	-2---------------- reset
			// An event with a dupe timestamp at the end of the reset window should not fire.
			window: 10,
			terms:  []string{"alpha"},
			reset: []ResetT{{
				Term: makeRaw("reset"),
			}}, // Simple relative reset
			steps: []step{
				{line: "alpha", stamp: 1},
				{line: "reset", stamp: 1},
			},
		},

		"SimpleNoReset": {
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
				{line: "beta", cb: matchStamps(5, 7, 4), postF: checkHotMask[InverseSet](0b100)},
				{line: "beta", postF: checkHotMask[InverseSet](0b110)},
			},
		},

		"WindowNoReset": {
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
				{line: "gamma", stamp: 9, postF: checkHotMask[InverseSet](0b100)},
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

		"DupeTimestampOnEndOfResetWindow": {
			//	-1---------------- alpha
			//	--2---------------- beta
			//	--3---------------- reset
			// An even with a dupe timestamp at the end of the reset window should not fire.
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{{
				Term: makeRaw("reset"),
			}}, // Simple relative reset
			steps: []step{
				{line: "alpha", stamp: 1},
				{line: "beta", stamp: 2},
				{line: "reset", stamp: 2},
			},
		},

		"ManualEval": {
			// -A-------------------
			// --B------------------
			// ---------------------
			// Create a match with an inverse that has a long reset window.
			clock:  0,
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("Shutdown initiated"),
					Window:   20,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "beta", postF: checkEval[*InverseSet](21, checkNoFire)}, // clock + rWindow == 21 within reset window
				{postF: checkEval[*InverseSet](22, matchStamps(1, 2))},         // clock + rWindow + 1== 22, outside reset window
			},
		},

		"SlideLeft": {
			// -----A--D----  Alpha
			// ------BC-----  Beta
			// -R-----------  Reset line
			//  *****         Reset Window
			// Slide left, deny first set, allow second set.
			// Should deny {A,B}, {A,C}, but allow {D,B}
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Slide:    -5,
					Window:   5,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "reset"},
				{line: "Match alpha.", stamp: 6},                        // clock + reset window, inside reset winow
				{line: "Match beta.", stamp: 7},                         // clock + reset window + 1, outside reset window
				{line: "Match beta.", stamp: 8},                         // clock + reset window + 2, outside reset window
				{line: "Match alpha.", stamp: 9, cb: matchStamps(9, 7)}, // clock + reset window + 3, should fire
			},
		},

		"SlideRight": {
			// -1---------5----
			// --2-------4-----
			// -----3----------
			// Should fail {1,2} on 3 but fire {5,4} after absolute timeout.
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Slide:    20,
					Window:   15,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "Match alpha."},
				{line: "Match beta."},                              // Should not fire due to future reset
				{line: "reset", stamp: 36},                         // reset window + slide + 1
				{line: "Match beta.", stamp: 36},                   // First term out of reset window
				{line: "Match alpha.", stamp: 37},                  // reset window + slide, should not fire
				{line: "NOOP", stamp: 71},                          // beta stamp + slide + window
				{line: "NOOP", stamp: 72, cb: matchStamps(37, 36)}, // beta stamp + slide + window+ 1, window expires
			},
		},

		"RelativeResetWindowMiss": {
			// -A-------------
			// --B------------
			// ---C-----------
			// -------------R-
			// Setup a relative reset window, and assert reset at end of window.  Should not fire.
			window: 3,
			terms:  []string{"alpha", "beta", "gamma"},
			reset: []ResetT{
				{
					Term:   makeRaw("reset"),
					Window: 10,
				},
			},
			steps: []step{
				{line: "Match alpha."},
				{line: "Match beta."},
				{line: "Match gamma", postF: checkEval[*InverseSet](2, checkNoFire)},
				{line: "Match reset", stamp: 11, postF: checkEval[*InverseSet](50, checkNoFire)},
			},
		},

		"AnchorRightHit": {
			// Absolute anchor on right term
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   60,
					Absolute: true,
					Anchor:   1, // Anchor on beta
				},
			},
			steps: []step{
				{line: "Match alpha."},
				{line: "Match beta.", stamp: 10},                  // No match due to inclusive right anchor
				{line: "NOOP", stamp: 70},                         // reset clock + reset window
				{line: "NOOP", stamp: 71, cb: matchStamps(1, 10)}, // reset clock + reset window
			},
		},

		"AnchorRightMiss": {
			// Absolute anchor on right term, fire a reset to prevent match.
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   60,
					Absolute: true,
					Anchor:   1, // Anchor on beta
				},
			},
			steps: []step{
				{line: "Match alpha."},
				{line: "Match beta.", stamp: 10}, // No match due to inclusive right anchor
				{line: "NOOP", stamp: 69},        // reset clock + reset window - 1
				{line: "reset", stamp: 70},       // reset clock + reset window + slide
				{line: "NOOP", stamp: 1000},      // reset clock + reset window
			},
		},

		"AnchorRightSlideHit": {
			// Absolute anchor on right term
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   60,
					Absolute: true,
					Anchor:   1, // Anchor on beta
					Slide:    5, // Slide window to the right
				},
			},
			steps: []step{
				{line: "Match alpha."},
				{line: "Match beta.", stamp: 10},                  // No match due to inclusive right anchor
				{line: "NOOP", stamp: 75},                         // reset clock + reset window + slide
				{line: "NOOP", stamp: 76, cb: matchStamps(1, 10)}, // reset clock + reset window + slide + 1
			},
		},

		"AnchorRightSlideMiss": {
			// Absolute anchor on right term, fire reset to prevent match.
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   60,
					Absolute: true,
					Anchor:   1, // Anchor on beta
					Slide:    5, // Slide window to the right
				},
			},
			steps: []step{
				{line: "Match alpha."},
				{line: "Match beta.", stamp: 10}, // No match due to inclusive right anchor
				{line: "NOOP", stamp: 74},        // reset clock + reset window  + slide - 1
				{line: "reset", stamp: 75},       // reset clock + reset window + slide
				{line: "NOOP", stamp: 1000},      // fire much later, still no match
			},
		},

		"AbsoluteRightAnchorLeftSlide": {
			// ---B--------------
			// -A----------------
			// ----C---D--E-------
			// --R---------------
			// Anchor absolute reset window with neg slide on line 2.
			// Should disallow {B,A,C} {B,A,D} but {B,A,E} should fire.
			window: 50,
			terms:  []string{"alpha", "beta", "gamma"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   5,
					Absolute: true,
					Anchor:   2,
					Slide:    -5,
				},
			},
			steps: []step{
				{line: "Match beta."},
				{line: "reset"},
				{line: "Match alpha."},
				{line: "Match gamma."},           // 'reset(2)' within  window of [-1, 5]
				{line: "Match gamma.", stamp: 8}, // 'reset(2)' outside window of [3,8], but won't fire until reset window expires
				{line: "Match gamma.", stamp: 11, cb: matchStamps(3, 1, 8)},
			},
		},

		"Relative": {
			//-1-3-------8-- alpha
			//--2--5-6----9- beta
			//----4--------- reset1
			//---------7---- reset2
			// Two relative resets; should be inclusive on the range.
			// {1,2} should fire.
			// {8,9} should fire
			window: 50,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{Term: makeRaw("reset1")},
				{Term: makeRaw("reset2")},
			},
			steps: []step{
				{line: "Match alpha."},
				{line: "Match beta."}, // Delay fire {1,2} until prove no dupes by assert stamp=3
				{line: "Match alpha part deux.", cb: matchStamps(1, 2)},
				{line: "This is reset1"},
				{line: "Match beta."},
				{line: "Match beta."},
				{line: "This is reset2"},
				{line: "Match alpha part trois."},
				{line: "beta again."}, // no match yet until out of reset2 window completely, which happens on next line
				{line: "NOOP", cb: matchStamps(8, 9)},
			},
		},

		"AbsoluteWithTwoRelativesHit": {
			// -1-------- alpha
			// -----2---- beta
			// ---------- reset1
			// ---------- reset2
			// ---------- reset3
			// Simple absolute window HIT test.
			// Should not fire until absolute window ends.
			// The two relative resets should not impact.
			window: 50,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{Term: makeRaw("reset1")},
				{Term: makeRaw("reset2")},
				{
					Term:     makeRaw("reset3"),
					Absolute: true,
					Window:   1000,
				},
			},
			steps: []step{
				{line: "Match alpha."},
				{line: "Match beta.", stamp: 51},
				{line: "NOOP", stamp: 1001},                         // alpha stamp + reset3 window
				{line: "NOOP", stamp: 1002, cb: matchStamps(1, 51)}, // alpha stamp + reset3 window + 1
			},
		},

		"AbsoluteWithTwoRelativesMiss": {
			// -1-------- alpha
			// -----2---- beta
			// ---------- reset1
			// ---------- reset2
			// ---------3 reset3
			// Simple absolute window HIT test.
			// Should not fire until absolute window ends.
			// The two relative resets should not impact.
			window: 50,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{Term: makeRaw("reset1")},
				{Term: makeRaw("reset2")},
				{
					Term:     makeRaw("reset3"),
					Absolute: true,
					Window:   1000,
				},
			},
			steps: []step{
				{line: "Match alpha."},
				{line: "Match beta.", stamp: 51},
				{line: "reset3", stamp: 1001}, // fire reset3 into absolute window
				{line: "NOOP", stamp: 10000},  // NOOP in the future, no match
			},
		},

		"PosRelativeOffsetHit": {
			// -1-------- alpha
			// -----2---- beta
			// ---------- reset1
			// ---------- reset2
			// ---------- reset3
			// Simple relative window HIT test.
			// Should not fire until relative window(s) end.
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{Term: makeRaw("reset1")},
				{Term: makeRaw("reset2")},
				{
					Term:     makeRaw("reset3"),
					Absolute: false,
					Window:   30,
				},
			},
			steps: []step{
				{line: "Match alpha."},
				{line: "Match beta.", stamp: 11},
				{line: "NOOP", stamp: 41},                         // reset3 window + relative window + 1 to include 'beta'
				{line: "NOOP", stamp: 42, cb: matchStamps(1, 11)}, //  Assert window expires
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
				{line: "gamma", postF: garbageCollect[*InverseSet](50)}, // window
				{postF: checkHotMask[InverseSet](0b110)},
				{postF: garbageCollect[*InverseSet](73)},
				{postF: checkHotMask[InverseSet](0b0)},
			},
		},

		"ResetTermsIgnoredOnNoMatch": {
			// ---------- alpha
			// ---------- beta
			// ---------- gamma
			// -123------ reset
			// Should not fire any resets.
			window: 10,
			terms:  []string{"alpha", "beta", "gamma"},
			reset: []ResetT{
				{Term: makeRaw("reset")},
			},
			steps: []step{
				{line: "reset"},
				{line: "reset"},
				{line: "reset", postF: checkResets[InverseSet](0, 0)},
			},
		},

		"SlideLeftResetsAreGCed": {
			// -------- alpha
			// -------- beta
			// --123--- reset
			// Create a set with a negative reset window.
			// Because there is a negative window, reset terms must
			// be kept around, but they should be GC'd after window.
			window: 50,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:   makeRaw("reset"),
					Slide:  -10,
					Window: 20,
				},
			},
			steps: []step{
				{line: "reset"},
				{line: "reset"},
				{line: "reset", postF: checkResets[InverseSet](0, 3)},
				{line: "NOOP", stamp: 72, postF: checkResets[InverseSet](0, 3)}, // window + reset window + 2 * abs(slide) + first reset + 1 for overlap
				{line: "NOOP", postF: checkResets[InverseSet](0, 2)},            // should peel off one reset
				{line: "NOOP", postF: checkResets[InverseSet](0, 1)},            // should peel off one reset
				{line: "NOOP", postF: checkResets[InverseSet](0, 0)},            // should peel off one reset
				{postF: checkGCMark[InverseSet](disableGC)},
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

		"SimpleWindowMatchHitWithAbsoluteResetAndBigJump": {
			// --A----------
			// ----------B--
			// Fire B outside of window, should delay until past window.
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   50,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "beta"},
				{line: "NOOP", stamp: 10000, cb: matchStamps(1, 2)}, // way out of reset window
			},
		},

		"SimpleNoResetDupeTerms": {
			// -1-2----------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha"},
			steps: []step{
				{line: "alpha"},
				{line: "alpha", cb: matchStamps(1, 2)},
			},
		},

		"SimpleDupeTermsWithReset": {
			// -1-2----------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   50,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "alpha"},
				{line: "NOOP", stamp: 51}, // reset window + 1
				{line: "NOOP", stamp: 52, cb: matchStamps(1, 2)},
			},
		},

		"SimpleDupeTermsWithResetFires": {
			// -1-2----------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   50,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "alpha"},
				{line: "reset", stamp: 51}, // reset window + 1
				{line: "NOOP", stamp: 52},  // should not fire due to reset at 51
			},
		},

		"SimpleNoResetDupeTermsSameTimestamp": {
			// -1-2----------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha"},
			steps: []step{
				{line: "alpha", stamp: 1},
				{line: "alpha", stamp: 1, cb: matchStamps(1, 1)},
			},
		},

		"SimpleDupeTermsSameTimestampWithReset": {
			// -1-2----------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   50,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "alpha", stamp: 1},
				{line: "alpha", stamp: 1},
				{line: "NOOP", stamp: 51}, // reset window + 1
				{line: "NOOP", stamp: 52, cb: matchStamps(1, 1)},
			},
		},

		"DupeNoResetsWithOtherTerms": {
			// -1---3-------------
			// ---2---------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha", "alpha", "beta"},
			steps: []step{
				{line: "alpha"},
				{line: "beta"},
				{line: "alpha"},
				{line: "alpha", cb: matchStamps(1, 3, 4, 2)},
			},
		},

		"DupeResetsWithOtherTerms": {
			// -1---3-------------
			// ---2---------------
			// Dupe terms are tolerated.
			window: 5,
			terms:  []string{"alpha", "alpha", "alpha", "beta"},
			reset: []ResetT{
				{
					Term: makeRaw("reset"),
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "beta"},
				{line: "alpha"},
				{line: "alpha"},
				{line: "NOOP", cb: matchStamps(1, 3, 4, 2)}, // Must wait until outside relative window to fire
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
				{line: "beta", postF: checkHotMask[InverseSet](0b10)},
				{line: "alpha", postF: checkHotMask[InverseSet](0b10)},
				{line: "beta"},
				{line: "alpha", stamp: 19},
				{line: "alpha", stamp: 19, cb: matchStamps(19, 19, 14)},
				{line: "nope", postF: checkHotMask[InverseSet](0b0)},
			},
		},

		"DupeTermWithOffsetAnchor": {
			// -1---3-------------
			// ---2---------------
			// Dupe terms are tolerated.
			window: 20,
			terms:  []string{"alpha", "alpha", "alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Anchor:   2,
					Window:   20,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "beta"},
				{line: "alpha", stamp: 11}, // This is the anchor term, reset will wait until window past this.
				{line: "alpha", stamp: 15},
				{line: "NOOP", stamp: 31}, // Should not fire, must be past anchor + window
				{line: "NOOP", stamp: 32, cb: matchStamps(1, 11, 15, 2)},
			},
		},

		"DupeTermWithOffsetAnchorResetFire": {
			// -1---3-------------
			// ---2---------------
			// Dupe terms are tolerated.
			window: 50,
			terms:  []string{"alpha", "alpha", "beta", "alpha"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Anchor:   2,
					Window:   20,
					Absolute: true,
				},
			},
			steps: []step{
				{line: "alpha"},
				{line: "beta"},
				{line: "alpha", stamp: 11}, // This is the anchor term, reset will wait until window past this.
				{line: "alpha", stamp: 15},
				{line: "reset", stamp: 31}, // Should not fire; should prune second alpha
				{line: "NOOP", stamp: 32},
				{line: "alpha"},
				{line: "alpha"},
				{line: "NOOP", stamp: 53},
				{line: "NOOP", stamp: 54, cb: matchStamps(1, 33, 34, 2)},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			sm, err := NewInverseSet(tc.window, makeTerms(tc.terms), tc.reset)
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

func TestSetInverseBadAnchor(t *testing.T) {
	var (
		window int64 = 10

		resets = []ResetT{
			{
				Term:   makeRaw("Shutdown initiated"),
				Anchor: 11, // Bad anchor
			},
		}
	)

	_, err := NewInverseSet(window, makeTermsA("alpha", "beta"), resets)
	if err != ErrAnchorRange {
		t.Fatalf("Expected err == ErrAnchorRange, got %v", err)
	}
}

func TestSetInverseEmptyTerm(t *testing.T) {
	term := TermT{Type: TermRaw, Value: ""}
	_, err := NewInverseSet(10, []TermT{term}, nil)
	if err != ErrTermEmpty {
		t.Fatalf("Expected err == ErrTermEmpty, got %v", err)
	}
}

func TestSetInverseEmptyResetTerm(t *testing.T) {
	term := TermT{Type: TermRaw, Value: "ok"}
	resetTerm := ResetT{Term: TermT{Type: TermRaw, Value: ""}}
	_, err := NewInverseSet(10, []TermT{term}, []ResetT{resetTerm})
	if err != ErrTermEmpty {
		t.Fatalf("Expected err == ErrTermEmpty, got %v", err)
	}
}

func TestSetInverseNoTerms(t *testing.T) {
	_, err := NewInverseSet(10, nil, nil)
	if err != ErrNoTerms {
		t.Fatalf("Expected err == ErrNoTerms, got %v", err)
	}
}

func TestSetInverseTooManyTerms(t *testing.T) {

	terms := make([]TermT, maxTerms)
	for i := range maxTerms {
		terms[i] = makeRaw(fmt.Sprintf("term %d", i))
	}
	_, err := NewInverseSet(10, terms, nil)
	if err != nil {
		t.Fatalf("Expected err == nil, got %v", err)
	}

	terms = append(terms, makeRaw("one too many"))

	_, err = NewInverseSet(10, terms, nil)
	if err != ErrTooManyTerms {
		t.Fatalf("Expected err == ErrTooManyTerms, got %v", err)
	}
}

// --------------------

func BenchmarkSetInverseMisses(b *testing.B) {
	sm, err := NewInverseSet(int64(time.Second), makeTermsA("frank", "burns"), nil)
	if err != nil {
		b.Fatalf("Expected err == nil, got %v", err)
	}

	noop := LogEntry{Line: "NOOP", Timestamp: time.Now().UnixNano()}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		noop.Timestamp += 1
		sm.Scan(noop)
	}
}

func BenchmarkSetInverseMissesWithReset(b *testing.B) {

	resets := []ResetT{
		{
			Term:     makeRaw("badterm"),
			Window:   1000,
			Absolute: true,
		},
	}

	sm, err := NewInverseSet(int64(time.Second), makeTermsA("frank", "burns"), resets)
	if err != nil {
		b.Fatalf("Expected err == nil, got %v", err)
	}

	noop := LogEntry{Line: "NOOP", Timestamp: time.Now().UnixNano()}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		noop.Timestamp += 1
		sm.Scan(noop)
	}
}

func BenchmarkSetInverseHitSequence(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	sm, err := NewInverseSet(int64(time.Second), makeTermsA("frank", "burns"), nil)
	if err != nil {
		b.Fatalf("Expected err == nil, got %v", err)
	}

	ts := time.Now().UnixNano()
	ev1 := LogEntry{Line: "Let's be frank"}
	ev2 := LogEntry{Line: "Mr burns I am"}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ev1.Timestamp = ts
		ev2.Timestamp = ts + 1
		ts += 2
		sm.Scan(ev1)
		m := sm.Scan(ev2)
		if m.Cnt != 1 {
			b.FailNow()
		}
	}
}

func BenchmarkSetInverseHitOverlap(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	sm, err := NewInverseSet(10, makeTermsA("frank", "burns"), []ResetT{{Term: makeRaw("reset1"), Window: 1, Absolute: true}})
	if err != nil {
		b.Fatalf("Expected err == nil, got %v", err)
	}

	var (
		ts  = time.Now().UnixNano()
		ev1 = LogEntry{Line: "Let's be frank"}
		ev2 = LogEntry{Line: "Mr burns I am"}
	)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ev1.Timestamp = ts
		sm.Scan(ev1)
		ts += 1
		ev1.Timestamp = ts
		sm.Scan(ev1)
		ts += 1
		ev1.Timestamp = ts
		sm.Scan(ev1)
		ts += 1
		ev2.Timestamp = ts
		ts += 1
		m := sm.Scan(ev2)
		if m.Cnt != 1 {
			b.FailNow()
		}
	}
}

func BenchmarkSetInverseRunawayMatch(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	sm, err := NewInverseSet(1000000, makeTermsA("frank", "burns"), nil)
	if err != nil {
		b.Fatalf("Expected err == nil, got %v", err)
	}

	var (
		ev1 = LogEntry{Line: "Let's be frank"}
	)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ev1.Timestamp += 1
		sm.Scan(ev1)
	}
}
