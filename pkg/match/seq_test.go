package match

import (
	"testing"
	"time"
)

func NewCasesSeqSimple() casesT {

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
			// -1-------- alpha
			// --2------- beta
			window: 10,
			terms:  []string{"alpha", "beta"},
			steps: []stepT{
				{line: "noop"},
				{line: "beta"},
				{line: "alpha"},
				{line: "beta", cb: matchStamps(3, 4)},
			},
		},

		"OverFire": {
			// -123-----
			// ----4----
			// Should fire *ONLY* {1,4},
			// not {2,4}, {3,4}
			window: 10,
			terms:  []string{"alpha", "beta"},
			steps: []stepT{
				{line: "alpha"},
				{line: "alpha"},
				{line: "alpha"},
				{line: "beta", cb: matchStamps(1, 4)},
			},
		},

		"Overlap": {
			// -12-4--7------
			// ---3--6--9----
			// -----5--8----A
			// Should fire {1,3,5}, {2,6,8}, {4,9,A}
			window: 20,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []stepT{
				{line: "alpha"},
				{line: "alpha"},
				{line: "beta"},
				{line: "alpha"},
				{line: "gamma", cb: matchStamps(1, 3, 5)},
				{line: "beta"},
				{line: "alpha"},
				{line: "gamma", cb: matchStamps(2, 6, 8)},
				{line: "beta"},
				{line: "noop"},
				{line: "noop"},
				{line: "noop"},
				{line: "gamma", cb: matchStamps(4, 9, 13)},
				{postF: garbageCollect(7 + 20)},     // GC up to event 7 + window; can't GC until past the window
				{postF: checkActive(1)},             // '7' Should still be sitting around
				{postF: garbageCollect(7 + 20 + 1)}, // Finish GC
				{postF: checkActive(0)},
			},
		},

		"SimpleWindow": {
			// -1------------ alpha
			// ------------2- beta
			// Second term is out of window; should not fire.
			window: 10,
			terms:  []string{"alpha", "beta"},
			steps: []stepT{
				{line: "alpha"},
				{line: "beta", stamp: 1 + 10 + 1, postF: checkActive(0)}, // alpha stamp + window + 1
			},
		},

		"SimpleWindow2": {
			// -1------------ alpha
			// ------------2- beta
			// Second term is out of window; should not fire.
			window: 10,
			terms:  []string{"alpha", "beta"},
			steps: []stepT{
				{line: "alpha"},
				{line: "alpha"},
				{line: "beta", stamp: 2 + 10 + 1, postF: checkActive(0)}, // beta stamp + window + 1
			},
		},

		"WindowOverlap": {
			// -A----C--E---F----- alpha
			// ---B---D-------G--- beta
			// Exercise various window overlaps.
			// Should fire {C,D} and {F,G}
			window: 20,
			terms:  []string{"alpha", "beta"},
			steps: []stepT{
				{line: "alpha"},
				{line: "noop", stamp: 1},
				{line: "noop", stamp: 1},
				{line: "noop", stamp: 1},
				{line: "beta", stamp: 1 + 20 + 1},
				{line: "alpha"},
				{line: "beta", cb: matchStamps(23, 24)},
				{line: "alpha", stamp: 25},
				{line: "alpha", stamp: 35},
				{line: "noop", stamp: 46},
				{line: "beta", cb: matchStamps(35, 47), postF: checkActive(0)},
			},
		},

		"DupeTimestamps": {
			// -1--------- alpha
			// -2--------- beta
			// -3--------- gamma
			// Demonstrate that we can match N copies of the same line
			window: 10,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []stepT{
				{line: "alpha1", stamp: 1},
				{line: "beta1", stamp: 1},
				{line: "gamma1", stamp: 1, cb: matchLines("alpha1", "beta1", "gamma1")},
			},
		},

		"GCOldTerms": {
			// -1------4--------------10----------
			// ---2--3----------8---9-----11----
			// ----------5--6-7---------------12-
			// Should fire {1,2,5}, {4,8,12}
			window: 50,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []stepT{
				{line: "alpha"},
				{line: "beta"},
				{line: "beta"},
				{line: "alpha"},
				{line: "gamma", cb: matchStamps(1, 2, 5)},
				{line: "gamma"},
				{line: "gamma"},
				{line: "beta"},
				{line: "beta"},
				{line: "alpha"},
				{line: "beta"},
				{line: "gamma", cb: matchStamps(4, 8, 12)},
				{postF: garbageCollect(12 + 50)}, // clock + window
				{postF: checkActive(0)},
			},
		},

		"ForceResetToNilAssertForCoverage": {
			window: 100,
			terms:  []string{"alpha", "beta", "gamma"},
			steps: []stepT{
				{line: "1_alpha"},
				{line: "2_beta"},
				{line: "3_beta"},
				{line: "4_beta"},
				{line: "5_beta"},
				{line: "6_beta"},
				{line: "7_gamma", cb: matchLines("1_alpha", "2_beta", "7_gamma")},
			},
		},

		"NOOPS": {
			window: 10,
			terms:  []string{"alpha", "beta"},
			steps: []stepT{
				{postF: checkEval(12345, checkNoFire)},
				{postF: garbageCollect(12345)},
			},
		},
	}
}

func NewCasesSeqDupes() casesT {
	return casesT{

		"Dupes": {
			// --1----3--4-5-6-------
			// --1----3--4-5-6-------
			// --1----3--4 5-6-------
			// ----2-----------7--89-
			// Because we are using a duplicate term, there is a possibility
			// of overlapping fire events.  This test should ensure that
			// the sequence matcher is able to handle this case.
			// Above should fire {1,3,4,7} and {3,4,5,8}
			window: 10,
			terms:  []string{"Discarding message", "Discarding message", "Discarding message", "Mnesia overloaded"},
			steps: []stepT{
				{line: "Discarding message"},
				{line: "Mnesia overloaded"},
				{line: "Discarding message"},
				{line: "Discarding message"},
				{line: "Discarding message"},
				{line: "Discarding message"},
				{line: "Mnesia overloaded", cb: matchStamps(1, 3, 4, 7)},
				{line: "Mnesia overloaded"},
				{line: "Mnesia overloaded", stamp: 6 + 10 + 1}, // Because dupe timestamps are consider matches in a sequence, window has to be past the last "Discarding message" to prevent fire
			},
		},

		"SingleLineDupes": {
			// -123----------- dupe
			// --23----------- dupe
			// ---3----------- dupe
			window: 10,
			terms:  []string{"dupe", "dupe", "dupe"},
			steps: []stepT{
				{line: "dupe1"},
				{line: "dupe2"},
				{line: "dupe3", cb: matchLines("dupe1", "dupe2", "dupe3")},
			},
		},

		"MultiLineDupes": {
			// -12----------- first
			// --2----------- first
			// ---34--------- second
			// ----4--------- second
			// Should fire {1,2,3,4}
			window: 10,
			terms:  []string{"first", "first", "second", "second"},
			steps: []stepT{
				{line: "first1"},
				{line: "first2"},
				{line: "second1"},
				{line: "second2", cb: matchLines("first1", "first2", "second1", "second2")},
			},
		},

		"FireMultiplesProperlyWithWindowMiss": {
			// -12345------------ dupe
			// --2345------------ dupe
			// ---345------------ dupe
			// ----------8------- fire
			window: 4,
			terms:  []string{"dupe", "dupe", "dupe", "fire"},
			steps: []stepT{
				{line: "dupe"}, //window [1,5]
				{line: "dupe"}, //window [2,6]
				{line: "dupe"}, //window [3,7]
				{line: "dupe"},
				{line: "dupe"},
				{line: "fire", stamp: 8}, // Should not fire  cause out of window.
			},
		},

		"FireMultiplesProperlyWithWindowHit": {
			// -1234567----------- dupe
			// --234567----------- dupe
			// ---34567----------- dupe
			// --------8---------- fire
			// Should fire {5,6,7,8}
			window: 3,
			terms:  []string{"dupe", "dupe", "dupe", "fire"},
			steps: []stepT{
				{line: "dupe1"},
				{line: "dupe2"},
				{line: "dupe3"},
				{line: "dupe4"},
				{line: "dupe5"},
				{line: "dupe6"},
				{line: "dupe7"},
				{line: "fire", stamp: 8, cb: matchLines("dupe5", "dupe6", "dupe7", "fire")},
			},
		},

		"FireMultiplesProperlyWithWindowHitSameTimestamp": {
			// -1234567----------- dupe
			// --234567----------- dupe
			// ---34567----------- dupe
			// --------89---------- fire
			// Should fire {1,2,3,8},{4,5,6,9} due to same timestamp
			window: 3,
			terms:  []string{"dupe", "dupe", "dupe", "fire"},
			steps: []stepT{
				{line: "dupe1", stamp: 1},
				{line: "dupe2", stamp: 1},
				{line: "dupe3", stamp: 1},
				{line: "dupe4", stamp: 1},
				{line: "dupe5", stamp: 1},
				{line: "dupe6", stamp: 1},
				{line: "dupe7", stamp: 1},
				{line: "fire1", stamp: 1, cb: matchLines("dupe1", "dupe2", "dupe3", "fire1")},
				{line: "fire2", stamp: 2, cb: matchLines("dupe4", "dupe5", "dupe6", "fire2")},
			},
		},

		"FireDisjointMultiplesSecondDuped": {
			// -12-456-89---------- dupe
			// --2-456-89---------- dupe
			// ---3---7------------ disjoint
			// ----456-89---------- dupe
			// ----456-89---------- dupe
			// ----------A--------- fire
			// Should fire {5,6,7,8,9,A}
			window: 5,
			terms:  []string{"dupe", "dupe", "disjoint", "dupe", "dupe", "fire"},
			steps: []stepT{
				{line: "1_dupe"},
				{line: "2_dupe"},
				{line: "3_disjoint"},
				{line: "4_dupe"},
				{line: "5_dupe"},
				{line: "6_dupe"},
				{line: "7_disjoint"},
				{line: "8_dupe"},
				{line: "9_dupe"},
				{line: "A_fire", cb: matchLines("5_dupe", "6_dupe", "7_disjoint", "8_dupe", "9_dupe", "A_fire")},
			},
		},

		"FireDisjointMultipleSecondNotDuped": {
			// -12-456-89---------- dupe
			// --2-456-89---------- dupe
			// ---3---7------------ disjoint
			// ----456-89---------- dupe
			// ----------A--------- fire
			// Should fire {5,6,7,8,9,A}
			window: 5,
			terms:  []string{"dupe", "dupe", "disjoint", "dupe", "fire"},
			steps: []stepT{
				{line: "1_dupe"},
				{line: "2_dupe"},
				{line: "3_disjoint"},
				{line: "4_dupe"},
				{line: "5_dupe"},
				{line: "6_dupe"},
				{line: "7_disjoint"},
				{line: "8_dupe"},
				{line: "9_dupe"},
				{line: "A_fire", cb: matchLines("5_dupe", "6_dupe", "7_disjoint", "8_dupe", "A_fire")},
			},
		},

		"FireDistinctMultiplesMiss": {
			// --1234----- alpha
			// ---234----- alpha
			// ------56--- beta
			// -------6--- beta
			// ---------A- fire
			// Should not fire; alpha line 2 is out of window.
			window: 4,
			terms:  []string{"alpha", "alpha", "beta", "beta", "fire"},
			steps: []stepT{
				{line: "1_alpha"},
				{line: "2_alpha"},
				{line: "3_alpha"},
				{line: "4_alpha"},
				{line: "5_beta"},
				{line: "6_beta"},
				{line: "8_fire", stamp: 8},
			},
		},

		"FireDistinctMultiplesHit": {
			// --12345----- alpha
			// ---2345----- alpha
			// -------678-- beta
			// --------78-- beta
			// ---------8-- fire
			// Should fire {3,4,6,7,8}.
			window: 5,
			terms:  []string{"alpha", "alpha", "beta", "beta", "fire"},
			steps: []stepT{
				{line: "1_alpha"},
				{line: "2_alpha"},
				{line: "3_alpha"},
				{line: "4_alpha"},
				{line: "5_alpha"},
				{line: "6_beta"},
				{line: "7_beta"},
				{line: "8_beta"},
				{line: "8_fire", stamp: 8, cb: matchLines("3_alpha", "4_alpha", "6_beta", "7_beta", "8_fire")},
			},
		},
	}
}

func TestSeq(t *testing.T) {

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
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {

			tc.cases.run(t, func(tc caseT) (Matcher, error) {
				return NewMatchSeq(tc.window, makeTerms(tc.terms)...)
			})

		})
	}
}

func TestSeqInitFail(t *testing.T) {

	cases := map[string]struct {
		err    error
		window int64
		terms  []TermT
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
			_, err := NewMatchSeq(tc.window, tc.terms...)
			if err != tc.err {
				t.Fatalf("Expected err == %v, got %v", tc.err, err)
			}
		})
	}
}

// ----------

func BenchmarkSequenceMisses(b *testing.B) {
	sm, err := NewMatchSeq(int64(time.Second), makeTermsA("frank", "burns")...)
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

func BenchmarkSequenceHitSequence(b *testing.B) {
	sm, err := NewMatchSeq(int64(time.Second), makeTermsA("frank", "burns")...)
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

func BenchmarkSequenceHitOverlap(b *testing.B) {
	sm, err := NewMatchSeq(int64(time.Second), makeTermsA("frank", "burns")...)
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

func BenchmarkSeqRunawayMatch(b *testing.B) {
	defer disableLogs()()
	sm, err := NewMatchSeq(1000000, makeTermsA("frank", "burns")...)
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
