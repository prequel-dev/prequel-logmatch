package scanner

import (
	"bufio"
	"errors"
	"strings"
	"testing"

	"github.com/prequel-dev/prequel-logmatch/pkg/format"
)

const (
	line1    = "2016-10-06T00:17:09.669794202Z stdout P log content 1\n"
	line2    = "2016-10-06T00:17:19.669794202Z stdout P log content 2\n"
	line3    = "2016-10-06T00:17:29.669794202Z stdout P log content 3\n"
	content1 = "log content 1"
	content2 = "log content 2"
	content3 = "log content 3"
)

func TestFold(t *testing.T) {

	tests := map[string]struct {
		derr    error
		input   string
		expect  []string
		doneCnt int
	}{
		"single_normal": {
			input:  line1,
			expect: []string{content1},
		},
		"double_normal": {
			input:  line1 + line2,
			expect: []string{content1, content2},
		},
		"triple_normal": {
			input:  line1 + line2 + line3,
			expect: []string{content1, content2, content3},
		},
		"garbage_line": {
			input: "garbage\n",
			derr:  format.ErrFormatDetect,
		},
		"garbage_followed_by_valid_line": {
			input:  "garbage\n" + line1,
			expect: []string{content1},
		},
		"garbage_valid_garbage": {
			input:  "garbage1\n" + line1 + "garbage2\n",
			expect: []string{content1 + "garbage2"},
		},
		"multi_garbage": {
			input:  line1 + "garbage2\n" + "garbage3\n",
			expect: []string{content1 + "garbage2" + "garbage3"},
		},
		"pre_garbage_plus_multi_garbage": {
			input:  "garbage1\n" + line1 + "garbage2\n" + "garbage3\n",
			expect: []string{content1 + "garbage2" + "garbage3"},
		},
		"multi_garbage_plus_good_line": {
			input:  line1 + "garbage2\n" + "garbage3\n" + line2 + line3,
			expect: []string{content1 + "garbage2" + "garbage3", content2, content3},
		},
		"emit last only once": {
			input:   line1,
			expect:  []string{content1},
			doneCnt: 1,
		},
		"emit last only once one hit": {
			input:   line1 + line2,
			expect:  []string{content1},
			doneCnt: 1,
		},
		"emit last only once two hits": {
			input:   line1 + line2 + line3,
			expect:  []string{content1, content2},
			doneCnt: 2,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {

			var (
				factory format.FactoryI
				derr    error
			)

			// Implement detect across multiple lines
			// (This code is in preq, should be we move it into format?)
			brdr := bufio.NewReader(strings.NewReader(tc.input))
			for i := 0; i < 3; i++ {
				line, err := brdr.ReadString('\n')
				if err != nil {
					break
				}
				if factory, _, derr = format.Detect(strings.NewReader(line)); derr == nil {
					break
				}
			}

			if tc.derr != nil {
				if !errors.Is(derr, tc.derr) {
					t.Errorf("Expected error %v, got %v", tc.derr, derr)
				}
				return
			} else if derr != nil {
				t.Fatalf("Detect() failed: %v", derr)
			}
			parser := factory.New()

			rdr := strings.NewReader(tc.input)

			var lines []string
			scanF := func(entry LogEntry) bool {

				lines = append(lines, entry.Line)
				switch {
				case tc.doneCnt > 1:
					tc.doneCnt--
				case tc.doneCnt == 1:
					tc.doneCnt--
					return true
				}

				return false
			}

			err := ScanForward(rdr, parser.ReadEntry, scanF, WithFold(true))
			if err != nil {
				t.Fatalf("ScanForward() failed: %v", err)
			}
			if len(lines) != len(tc.expect) {
				t.Fatalf("Expected %d lines, got %d", len(tc.expect), len(lines))
			}
			for i, line := range lines {
				if line != tc.expect[i] {
					t.Errorf("Expected line %d to be %q, got %q", i, tc.expect[i], line)
				}
			}
		})
	}
}
