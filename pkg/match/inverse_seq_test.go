package match

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func NewCasesSeqResets() casesT {
	return casesT{

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
			steps: []stepT{
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
			steps: []stepT{
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
			steps: []stepT{
				{line: "alpha", stamp: 1},
				{line: "reset", stamp: 1},
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
			steps: []stepT{
				{line: "alpha", stamp: 1},
				{line: "beta", stamp: 2},
				{line: "reset", stamp: 2},
			},
		},

		"SimpleWindowMatchWithAbsoluteReset": {
			// --A----------
			// ----------B--
			// Fire B inside window, should delay until past window.
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   50,
					Absolute: true,
				},
			},
			steps: []stepT{
				{line: "alpha", stamp: 1},
				{line: "beta", stamp: 1 + 10},                             // alpha stamp + window + 1
				{line: "NOOP", stamp: 1 + 50},                             // still in absolute reset window},
				{line: "NOOP", stamp: 1 + 50 + 1, cb: matchStamps(1, 11)}, // alpha stamp + window + reset window + 1
			},
		},

		"SimpleWindowMatchHitWithAbsoluteResetAndBigJump": {
			// --A----------
			// ---B--------
			// Fire B inside window, should delay until past reset window.
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   50,
					Absolute: true,
				},
			},
			steps: []stepT{
				{line: "alpha"},
				{line: "beta"},
				{line: "NOOP", stamp: 10000, cb: matchStamps(1, 2)}, // way out of reset window
			},
		},

		"SimpleWindowMatchMissWithAbsoluteReset": {
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
			steps: []stepT{
				{line: "alpha", stamp: 1},
				{line: "beta", stamp: 1 + 10 + 1}, // out of the window
				{line: "NOOP", stamp: 10000},      // way out of reset window
			},
		},

		"SlideLeft": {
			// -------23----- alpha
			// ---------4---- beta
			// -1------------ reset
			// Fire a reset with a left shift,
			// should deny {2,4} on winow, but allow {3,4}
			window: 5,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Slide:    -5,
					Window:   20,
					Absolute: true,
				},
			},
			steps: []stepT{
				{line: "reset"},
				{line: "alpha", stamp: 6}, // reset window [1,21], no fire
				{line: "alpha"},           // reset window [2,22], should fire after 22
				{line: "beta"},
				{line: "noop", stamp: 22}, // no fire until outside reset window
				{line: "noop", cb: matchStamps(7, 8)},
			},
		},

		"SlideRight": {
			// ---12---------- alpha
			// -----3--------- beta
			// --------------4- reset
			// Fire a reset with a right shift,
			// should deny {2,4} on winow, but allow {3,4}
			window: 5,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Slide:    5,
					Window:   20,
					Absolute: true,
				},
			},
			steps: []stepT{
				{line: "alpha"},            // reset window [6,26], no fire
				{line: "alpha", stamp: 22}, // reset window [27,47], should fire after 47
				{line: "reset"},            // reset ignored because abs window is slide right
				{line: "beta"},
				{line: "reset", stamp: 26}, // right edge of line 1 window
				{line: "noop", stamp: 47},  // right edge of line 2 window
				{line: "noop", cb: matchStamps(22, 24)},
				{line: "noop", stamp: 1000}, // way out of reset window, should not fire
			},
		},

		"RelativeResetWindowMiss": {
			// -A-------------
			// --B------------
			// ---C-----------
			// -------------R-
			// Setup a relative reset window, and assert reset at end of window.
			// Should not fire.
			window: 3,
			terms:  []string{"alpha", "beta", "gamma"},
			reset: []ResetT{
				{
					Term:   makeRaw("reset"),
					Window: 10,
				},
			},
			steps: []stepT{
				{line: "alpha"},
				{line: "beta"},
				{line: "gamma"},                // should fire but delay on on reset window [1,13]
				{line: "noop", stamp: 13},      // Noop on edge of window, but can't fire until 13+1
				{line: "reset", stamp: 3 + 10}, // reset on right edge window
				{line: "noop", stamp: 1000},    // way out of reset window, should not fire
			},
		},

		"SlideAnchor": {
			// -1------------ alpha
			// ----2--------- beta
			window: 3,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   10,
					Absolute: true,
					Anchor:   1,
				},
			},
			steps: []stepT{
				{line: "alpha"},
				{line: "beta", stamp: 1 + 3}, //  clock + window, reset window [4, 14]
				{line: "noop", stamp: 14},    // no fire until after window
				{line: "noop", cb: matchStamps(1, 4)},
			},
		},

		"AbsSlideResetContinue": {
			// -A-----------
			// ---B---------
			// ----C--DE----
			// --R----------
			// Anchor absolute reset window with neg slide on line 2.
			// Should disallow [A,B,C] and [A,B,D] but [A,B,E] should fire.
			window: 10,
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
			steps: []stepT{
				{line: "alpha"},
				{line: "reset"},
				{line: "beta"},
				{line: "gamma"},           // reset window [-1, 4], no fire
				{line: "gamma", stamp: 7}, // reset window [2, 7], no fire
				{line: "gamma", stamp: 8}, // reset window [3, 8], should fire on 9
				{line: "noop", stamp: 9, cb: matchStamps(1, 3, 8)},
			},
		},

		"Relative": {
			// -1-3---8-A--- alpha
			// --2--56-9-B-- beta
			// ----4-------- reset1
			// ----------C-- reset2
			window: 50,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{Term: makeRaw("reset1")},
				{Term: makeRaw("reset2")},
			},
			steps: []stepT{
				{line: "alpha"},
				{line: "beta"}, // Should match, but cannot fire until next event due to reset
				{line: "alpha", cb: matchStamps(1, 2)},
				{line: "reset1"},
				{line: "beta"},
				{line: "beta"},
				{line: "noop"},
				{line: "alpha"},
				{line: "beta"}, // Should match, but cannot fire until next event due to reset
				{line: "alpha", cb: matchStamps(8, 9)},
				{line: "beta"},
				{line: "reset2", stamp: 11}, // same timestamp as 11, should deny [10,11]
				{line: "noop", stamp: 1000},
			},
		},

		"AbsoluteHit": {
			window: 50,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{Term: makeRaw("reset1")},
				{Term: makeRaw("reset2")},
				{
					Term:     makeRaw("reset3"),
					Window:   100,
					Absolute: true,
				},
			},
			steps: []stepT{
				{line: "alpha"},
				{line: "beta", stamp: 1 + 50},                      // clock + window, reset1 [1, 51], reset2 [1, 51], reset3 [1, 101]
				{line: "NOOP", stamp: 101},                         // no fire until after window
				{line: "NOOP", stamp: 102, cb: matchStamps(1, 51)}, // fire after reset window
			},
		},

		"AbsoluteMiss": {
			window: 50,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{Term: makeRaw("reset1")},
				{Term: makeRaw("reset2")},
				{
					Term:     makeRaw("reset3"),
					Window:   100,
					Absolute: true,
				},
			},
			steps: []stepT{
				{line: "alpha"},
				{line: "beta", stamp: 1 + 50}, // clock + window, reset1 [1, 51], reset2 [1, 51], reset3 [1, 101]
				{line: "reset3", stamp: 101},  // reset at edge of window
				{line: "NOOP", stamp: 1000},   // no fire
			},
		},

		"ManualEval": {
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   20,
					Absolute: true,
				},
			},
			steps: []stepT{
				{line: "alpha"},
				{line: "beta"},
				{line: "gamma"}, // Cannot fire until after reset window
				{postF: checkEval(21, checkNoFire)},
				{postF: checkEval(22, matchStamps(1, 2))},
			},
		},

		"PosRelativeOffset": {
			window: 10,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{Term: makeRaw("reset1")},
				{Term: makeRaw("reset2")},
				{
					Term:     makeRaw("reset3"),
					Absolute: false,
					Window:   5,
				},
			},
			steps: []stepT{
				{line: "alpha"},
				{line: "beta"}, // reset1: [1,2] reset2: [1,2] reset3: [1,7]; cannot fire until after reset3
				{line: "noop", stamp: 7},
				{line: "noop", stamp: 8, cb: matchStamps(1, 2)},
				{line: "noop", stamp: 1000},
			},
		},

		"DupesWithResetHit": {
			// -123---------
			// -123---------
			// -123---------
			// ----4----56--
			window: 10,
			terms: []string{
				"alpha",
				"alpha",
				"alpha",
				"beta",
			},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   20,
					Absolute: true,
				},
			},
			steps: []stepT{
				{line: "alpha"},
				{line: "alpha"},
				{line: "alpha"},
				{line: "beta"},
				{line: "beta", stamp: 21},
				{line: "beta", stamp: 22, cb: matchStamps(1, 2, 3, 4)},
				{line: "noop", stamp: 1000},
			},
		},

		"DupesWithResetMiss": {
			// -123--------- alpha
			// -123--------- alpha
			// -123--------- alpha
			// ----4-----6-- beta
			// ---------5--- reset
			window: 10,
			terms: []string{
				"alpha",
				"alpha",
				"alpha",
				"beta",
			},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   20,
					Absolute: true,
				},
			},
			steps: []stepT{
				{line: "alpha"},
				{line: "alpha"},
				{line: "alpha"},
				{line: "beta"},
				{line: "reset", stamp: 21},
				{line: "beta", stamp: 22},
				{line: "noop", stamp: 1000},
			},
		},

		"DupesWithResetMissOnAnchor": {
			// -123--------- alpha
			// -123--------- alpha
			// -123--------- alpha
			// ----4-----6-- beta
			// ---------5--- reset
			window: 10,
			terms: []string{
				"alpha",
				"alpha",
				"alpha",
				"beta",
			},
			reset: []ResetT{
				{
					Term:     makeRaw("reset"),
					Window:   20,
					Anchor:   2,
					Absolute: true,
				},
			},
			steps: []stepT{
				{line: "alpha1"},
				{line: "alpha2"},
				{line: "alpha3"},
				{line: "beta1"},
				{line: "reset", stamp: 21},
				{line: "beta2", stamp: 22},
				{line: "noop", stamp: 1000},
			},
		},

		"ResetsIgnoreOnNoMatch": {
			window: 10,
			terms:  []string{"alpha", "beta", "gamma"},
			reset:  []ResetT{{Term: makeRaw("reset")}},
			steps: []stepT{
				{line: "reset"},
				{line: "reset"},
				{line: "reset"},
				{postF: checkResets(0, 0)},
			},
		},

		"NegativesAreGCed": {
			window: 50,
			terms:  []string{"alpha", "beta", "gamma"},
			reset: []ResetT{
				{
					Term:   makeRaw("reset"),
					Slide:  -10,
					Window: 20,
				},
			},
			steps: []stepT{
				{line: "reset"},
				{line: "reset"},
				{line: "reset", postF: checkResets(0, 3)},                    // Reset terms with nothing hot w/o lookback have been optimized out.
				{line: "NOOP", stamp: 1 + 50 + 20, postF: checkResets(0, 3)}, // Emit noop at full GC window (see calcGCWindow)}, should have some negative terms
				{line: "NOOP", postF: checkResets(0, 2)},                     // Emit noop right after window, should have peeled off one term
				{line: "NOOP", postF: checkResets(0, 1)},                     // Emit noop right after window, should have peeled off one term
				{line: "NOOP", postF: checkResets(0, 0)},                     // Emit noop right after window, should have peeled off one term
				{postF: checkGCMark(disableGC)},
			},
		},

		"SimpleResetWindow": {
			window: 2,
			terms:  []string{"alpha", "beta"},
			reset: []ResetT{
				{
					Term: makeRaw("reset"),
				},
			},
			steps: []stepT{
				{line: "alpha"},
				{line: "reset", stamp: 1},
				{line: "NOOP", stamp: 3, postF: checkResets(0, 1)},
				{line: "NOOP", stamp: 4, postF: checkResets(0, 0)},
			},
		},

		"ResetDupesWithAnchorFire": {
			window: 5,
			terms:  []string{"alpha", "alpha", "alpha"},
			reset: []ResetT{
				{
					Term:   makeRaw("reset"),
					Anchor: 2,
				},
			},
			steps: []stepT{
				{line: "alpha1"},
				{line: "alpha2"},
				{line: "alpha3"},
				{line: "nope4"}, // Shouldn't fire yet. Reset anchor is on line 2. So reset range is 3 + 3-1 == 5)
				{line: "nope5"}, // Not yet my friend
				{line: "nope6", cb: matchStamps(1, 2, 3)}, // Fire on stamp 6 >  reset window 2-5
				{line: "alpha7"}, // Normally {2,3,7} would fire, but must wait for anchor at {7, 7+7-2==12}
				{line: "alpha8"}, // Normally 3,7,8 would fire, but must wait for {3, 8+8-3==13}
				{line: "alpha12", stamp: 13, cb: matchStamps(2, 3, 7)},
				{line: "nope14", stamp: 14, cb: matchStamps(3, 7, 8)},
			},
		},

		"ResetDupesWithAnchorMiss": {
			window: 5,
			terms:  []string{"alpha", "alpha", "alpha"},
			reset: []ResetT{
				{
					Term:   makeRaw("reset"),
					Anchor: 2,
				},
			},
			steps: []stepT{
				{line: "alpha1"},
				{line: "alpha2"},
				{line: "alpha3"},
				{line: "nope4"},  // Shouldn't fire yet. Reset anchor is on line 2. So reset range is 3 + 3-1 == 5)
				{line: "reset5"}, // Reset at stamp 5 will deny {1,2,3}
				{line: "nope6"},  // No fire, but 2,3,7 still active
				{line: "alpha7"}, // Normally {2,3,7} would fire, but must wait for anchor at {7, 7+7-2==12}
				{line: "alpha8"}, // Normally 3,7,8 would fire, but must wait for {3, 8+8-3==13}
				{line: "alpha12", stamp: 13, cb: matchStamps(2, 3, 7)},
				{line: "nope14", stamp: 14, cb: matchStamps(3, 7, 8)},
			},
		},
	}
}

func TestInverseSeq(t *testing.T) {

	cases := map[string]struct {
		cases casesT
	}{
		"Single": {
			cases: NewCasesSingle(),
		},
		"Simple": {
			cases: NewCasesSeqSimple(),
		},
		"Dupes": {
			cases: NewCasesSeqDupes(),
		},
		"Resets": {
			cases: NewCasesSeqResets(),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {

			tc.cases.run(t, func(tc caseT) (Matcher, error) {
				return NewInverseSeq(tc.window, makeTerms(tc.terms), tc.reset)
			})

		})
	}
}

func TestInverseSeqInitFail(t *testing.T) {

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

		"EmptyResetTerm": {
			err:    ErrTermEmpty,
			window: 10,
			terms:  makeTermsA("ok"),
			reset:  []ResetT{{Term: TermT{Type: TermRaw, Value: ""}}},
		},

		"BadAnchor": {
			err: ErrAnchorRange,

			window: 10,
			terms:  makeTermsA("alpha", "beta"),
			reset: []ResetT{
				{
					Term:   makeRaw("Shutdown initiated"),
					Anchor: 11, // Bad anchor
				},
			},
		},

		"AlmostTooManyTerms": {
			err:    nil,
			window: 10,
			terms:  makeTermsN(maxTerms),
		},

		"DupeShouldNotPushOverMax": {
			err:    ErrTooManyTerms, // Not supported yet on InversetSeq
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
			_, err := NewInverseSeq(tc.window, tc.terms, tc.reset)
			if err != tc.err {
				t.Fatalf("Expected err == %v, got %v", tc.err, err)
			}
		})
	}
}

// // --------------------

func BenchmarkSeqInverseMisses(b *testing.B) {
	sm, err := NewInverseSeq(int64(time.Second), makeTermsA("frank", "burns"), nil)
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

func BenchmarkSeqInverseMissesWithReset(b *testing.B) {

	resets := []ResetT{
		{
			Term:     makeRaw("badterm"),
			Window:   1000,
			Absolute: true,
		},
	}

	sm, err := NewInverseSeq(int64(time.Second), makeTermsA("frank", "burns"), resets)
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

func BenchmarkSeqInverseHitSequence(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	sm, err := NewInverseSeq(int64(time.Second), makeTermsA("frank", "burns"), nil)
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

func BenchmarkSeqInverseHitOverlap(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	sm, err := NewInverseSeq(10, makeTermsA("frank", "burns"), nil)
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

func BenchmarkSeqInverseRunawayMatch(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	sm, err := NewInverseSeq(1000000, makeTermsA("frank", "burns"), nil)
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
