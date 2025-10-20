package match

import (
	"testing"

	"github.com/rs/zerolog"
)

func NewCasesSingle() casesT {

	return casesT{
		"SingleTerm": {
			// -A---------------- alpha
			window: 10,
			terms:  []string{"alpha"},
			steps: []stepT{
				{line: "alpha", cb: matchStamps(1)},
				{line: "beta"},
			},
		},

		"NOOPS": {
			window: 10,
			terms:  []string{"alpha"},
			steps: []stepT{
				{postF: checkEval(12345, checkNoFire)},
				{postF: garbageCollect(12345)},
			},
		},
	}
}

func TestSingle(t *testing.T) {

	cases := NewCasesSingle()
	cases.run(t, func(tc caseT) (Matcher, error) {
		return NewMatchSingle(makeTerms(tc.terms)[0])
	})
}

func TestSingleInitFail(t *testing.T) {

	cases := map[string]struct {
		err  error
		term TermT
	}{

		"EmptyTerm": {
			err:  ErrTermEmpty,
			term: TermT{Type: TermRaw, Value: ""},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := NewMatchSingle(tc.term)
			if err != tc.err {
				t.Fatalf("Expected err == %v, got %v", tc.err, err)
			}
		})
	}
}

func disableLogs() func() {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	return func() {
		zerolog.SetGlobalLevel(level)
	}
}

func BenchmarkSingleMiss(b *testing.B) {
	defer disableLogs()()

	sm, err := NewMatchSingle(TermT{Type: TermRaw, Value: "shrubbery"})
	if err != nil {
		b.Fatalf("Expected err == nil, got %v", err)
	}

	var (
		clock int64
		ev1   = LogEntry{Line: "nope"}
	)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		clock++
		ev1.Timestamp = clock
		sm.Scan(ev1)
	}
}

func BenchmarkSingleHit(b *testing.B) {
	defer disableLogs()()

	sm, err := NewMatchSingle(TermT{Type: TermRaw, Value: "shrubbery"})
	if err != nil {
		b.Fatalf("Expected err == nil, got %v", err)
	}

	var (
		clock int64
		ev1   = LogEntry{Line: "Bring me a shrubbery"}
	)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		clock++
		ev1.Timestamp = clock
		sm.Scan(ev1)
	}
}
