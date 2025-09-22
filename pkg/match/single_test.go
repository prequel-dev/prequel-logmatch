package match

import (
	"testing"
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
