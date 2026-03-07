package scanner

import (
	"testing"
)

func TestFixupUTF16(t *testing.T) {

	tests := map[string]struct {
		data string
		hits []int
		want []int
	}{
		"ascii": {
			data: "abc",
			hits: []int{1, 2},
			want: []int{1, 2}, // ascii is 1 for 1
		},
		"euro": {
			data: "€abc",
			hits: []int{4, 5},
			want: []int{2, 3},
		},
		"euroeuroeuro": {
			data: "€€€abc",
			hits: []int{10, 11},
			want: []int{4, 5},
		},
		"matcheuro": {
			data: "€abc",
			hits: []int{0, 3},
			want: []int{0, 1},
		},
		"matcheuroanda": {
			data: "€abc",
			hits: []int{0, 4},
			want: []int{0, 2},
		},
		"surrogate_poop_single": {
			data: "💩",
			hits: []int{0, 3},
			want: []int{0, 2},
		},
		"surrogate_poop": {
			data: "💩abc",
			hits: []int{5, 6},
			want: []int{3, 4},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {

			nHits := hitToUtf16(tc.data, tc.hits)
			if nHits[0] != tc.want[0] {
				t.Errorf("First offset wrong want:%v got:%v", tc.want[0], nHits[0])
			}
			if nHits[1] != tc.want[1] {
				t.Errorf("Second offset wrong want:%v got:%v", tc.want[1], nHits[1])
			}
		})
	}
}

func TestHitsToUtf16(t *testing.T) {
	// Valid hits
	hits := [][]int{{1, 2}, {2, 3}}
	s := "abc"
	nHits := hitsToUtf16(s, hits)
	if len(nHits) != 2 {
		t.Errorf("hitsToUtf16 should return 2 slices")
	}
	// Invalid hit size
	badHits := [][]int{{1}}
	if hitsToUtf16(s, badHits) != nil {
		t.Errorf("hitsToUtf16 should return nil for bad hit size")
	}
	// Invalid hit values
	badHits2 := [][]int{{2, 1}}
	if hitsToUtf16(s, badHits2) != nil {
		t.Errorf("hitsToUtf16 should return nil for bad hit values")
	}
	// Out of bounds
	badHits3 := [][]int{{0, 4}}
	if hitsToUtf16(s, badHits3) != nil {
		t.Errorf("hitsToUtf16 should return nil for out of bounds")
	}
}
