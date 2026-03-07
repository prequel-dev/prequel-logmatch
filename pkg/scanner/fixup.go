package scanner

import (
	"unicode/utf8"

	"github.com/rs/zerolog/log"
)

// Expects utf8 offset hits for corresponding string 's'
// - len(hit) == 2
// - hit[0] < hit[1]
// - hit[1] <= len(s)
// Returns new slice with hits aligned to corresponding utf16 characters

func hitToUtf16(s string, hit []int) []int {

	var (
		i           int
		utf16Offset int
	)

	scan := func(utf8Offset int) int {
		for i < utf8Offset {
			r, sz := utf8.DecodeRuneInString(s[i:])

			// If rune in the BMP (U+0000 to U+FFFF), counts as one UTF16 character.
			// Otherwise, increment by 2 for surrogate pairs
			if r >= 0x10000 {
				utf16Offset += 2
			} else {
				utf16Offset += 1
			}

			// next rune
			i += sz
		}
		return utf16Offset
	}
	v1 := scan(hit[0])
	v2 := scan(hit[1])

	return []int{v1, v2}
}

func hitsToUtf16(s string, hits [][]int) [][]int {
	nHits := make([][]int, 0, len(hits))

	szLine := len(s)
	for _, hit := range hits {
		if len(hit) != 2 {
			log.Warn().
				Int("len(hit)", len(hit)).
				Msg("Unexpected hit size from FindAllStringIndex")
			return nil
		} else if hit[0] >= hit[1] || hit[1] > szLine {
			log.Warn().
				Int("sz", szLine).
				Int("hit[0]", hit[0]).
				Int("hit[1]", hit[1]).
				Msg("Unexpected hit values from FindAllStringIndex")
			return nil
		}
		nHits = append(nHits, hitToUtf16(s, hit))
	}

	return nHits
}
