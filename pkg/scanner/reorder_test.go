package scanner

import (
	"fmt"
	"testing"
)

func TestRecordBadParams(t *testing.T) {
	tests := map[string]struct {
		err    error
		window int64
		cb     ScanFuncT
	}{
		"zero window": {
			err:    ErrInvalidWindow,
			window: 0,
			cb:     nil,
		},
		"negative window": {
			err:    ErrInvalidWindow,
			window: -1,
			cb:     nil,
		},
		"nil callback": {
			err:    ErrInvalidCallback,
			window: 10,
			cb:     nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rw, err := NewReorder(tc.window, tc.cb)
			if rw != nil {
				t.Fatalf("Expected nil reorder, got %v", rw)
			}
			if err != tc.err {
				t.Fatalf("Expected error %v, got %v", tc.err, err)
			}
		})
	}
}

func TestReorder(t *testing.T) {

	type stepT struct {
		stamp  int64
		expect []int64
	}

	const flushStamp = -1

	step := func(stamp int64, expect ...int64) stepT {
		return stepT{stamp: stamp, expect: expect}
	}

	flush := func(expect ...int64) stepT {
		return step(flushStamp, expect...)
	}

	steps := func(st ...stepT) []stepT {
		return st
	}

	tests := map[string]struct {
		dmark  int
		window int64
		steps  []stepT
		opts   []ROpt
	}{
		"empty": {},
		"single": {
			steps: steps(step(1)),
		},
		"single flush": {
			steps: steps(step(1), flush(1)),
		},
		"single window": {
			steps: steps(step(1), step(11, 1)),
		},
		"double": {
			steps: steps(step(1), step(2)),
		},
		"double flush": {
			steps: steps(step(1), step(2), flush(1, 2)),
		},
		"double window": {
			steps: steps(step(1), step(2), step(12, 1, 2)),
		},
		"double split window": {
			steps: steps(step(1), step(2), step(11, 1), step(12, 2)),
		},
		"dupe": {
			steps: steps(step(1), step(1)),
		},
		"dupe flush": {
			steps: steps(step(1), step(1), flush(1, 1)),
		},
		"dupe window": {
			steps: steps(step(1), step(1), step(11, 1, 1)),
		},
		"simple reorder": {
			steps: steps(step(1), step(3), step(2), flush(1, 2, 3)),
		},
		"simple reorder window": {
			steps: steps(step(1), step(3), step(2), step(13, 1, 2, 3)),
		},
		"simple reorder window swap delivery": {
			steps: steps(step(1), step(2), step(3), step(13, 1, 2, 3)),
		},
		"simple reorder split window": {
			steps: steps(step(1), step(3), step(2), step(10), step(11, 1), step(13, 2, 3)),
		},
		"simple double reorder": {
			steps: steps(step(1), step(4), step(3), step(2), flush(1, 2, 3, 4)),
		},
		"simple double reorder window": {
			steps: steps(step(1), step(4), step(3), step(2), step(14, 1, 2, 3, 4)),
		},
		"simple double reorder split window": {
			steps: steps(step(1), step(4), step(3), step(2), step(12, 1, 2), step(13, 3), step(14, 4)),
		},
		"simple double dupe reorder": {
			steps: steps(step(1), step(4), step(3), step(3), flush(1, 3, 3, 4)),
		},
		"triple reorder": {
			steps: steps(step(1), step(5), step(4), step(3), step(2), flush(1, 2, 3, 4, 5)),
		},
		"triple reorder swap delivery": {
			steps: steps(step(1), step(5), step(2), step(3), step(4), flush(1, 2, 3, 4, 5)),
		},
		"triple reorder mixed delivery": {
			steps: steps(step(1), step(5), step(2), step(4), step(3), flush(1, 2, 3, 4, 5)),
		},
		"entry just within window should deliver": {
			steps: steps(step(11), step(1), flush(1, 11)),
		},
		"entry outside window should drop": {
			steps: steps(step(12), step(1), flush(12)),
		},
		"done in order": {
			steps: steps(step(1), step(2), step(3), step(11, 1)),
			dmark: 1,
		},
		"done out of order 1": {
			steps: steps(step(1), step(3), step(2), step(14, 1)),
			dmark: 1,
		},
		"done out of order 2": {
			steps: steps(step(1), step(3), step(2), step(14, 1, 2)),
			dmark: 2,
		},
		"done out of order 3": {
			steps: steps(step(1), step(3), step(2), step(14, 1, 2, 3)),
			dmark: 3,
		},
		"done double reorder split window": {
			steps: steps(step(1), step(4), step(3), step(2), step(12, 1, 2)),
			dmark: 2,
		},
		"memory limit in order": {
			steps: steps(step(1), step(2, 1)),
			opts:  []ROpt{WithMemoryLimit(100)},
		},
		"memory limit in order with done": {
			steps: steps(step(1), step(2, 1)),
			opts:  []ROpt{WithMemoryLimit(100)},
			dmark: 1,
		},
		"memory limit out of order": {
			steps: steps(step(5), step(2, 2)),
			opts:  []ROpt{WithMemoryLimit(100)},
		},
		"memory limit out of order with done": {
			steps: steps(step(5), step(2, 2)),
			opts:  []ROpt{WithMemoryLimit(100)},
			dmark: 1,
		},
		"memory limit multiple out of order ": {
			steps: steps(step(5), step(2, 2), step(3, 3)),
			opts:  []ROpt{WithMemoryLimit(100)},
		},
		"memory limit multiple out of order with done": {
			steps: steps(step(5), step(2, 2), step(3, 3)),
			opts:  []ROpt{WithMemoryLimit(100)},
			dmark: 2,
		},
		"wide memory limit in order": {
			steps: steps(step(1), step(2), step(3), step(4, 1)),
			opts:  []ROpt{WithMemoryLimit(300)},
		},
		"wide memory limit in order with done": {
			steps: steps(step(1), step(2), step(3), step(4, 1)),
			opts:  []ROpt{WithMemoryLimit(300)},
			dmark: 1,
		},
		"wide memory limit out of order": {
			steps: steps(step(5), step(4), step(2), step(3, 2)),
			opts:  []ROpt{WithMemoryLimit(300)},
		},
		"wide memory limit out of order with done": {
			steps: steps(step(5), step(4), step(2), step(3, 2), step(6, 3)),
			opts:  []ROpt{WithMemoryLimit(300)},
			dmark: 2,
		},
		"memory limit shifts window to the right": {
			steps: steps(step(10), step(11), step(12), step(13, 10), step(9), flush(11, 12, 13)),
			opts:  []ROpt{WithMemoryLimit(300)},
		},
		"memory limit shift huge limit": {
			steps:  steps(step(10), step(11), step(12, 10), step(13, 11), step(14, 12), step(15, 13)),
			opts:   []ROpt{WithMemoryLimit(200)},
			window: 1000,
		},
		"memory limit shift huge limit done": {
			steps:  steps(step(10), step(11), step(12, 10), step(13, 11), step(14, 12), step(15, 13)),
			opts:   []ROpt{WithMemoryLimit(200)},
			window: 1000,
			dmark:  4,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {

			window := tc.window
			if window == 0 {
				window = 10
			}

			markCnt := 0
			var entries []LogEntry
			cb := func(entry LogEntry) bool {
				entries = append(entries, entry)
				markCnt += 1
				return tc.dmark > 0 && markCnt >= tc.dmark
			}

			rw, err := NewReorder(10, cb, tc.opts...)
			if err != nil {
				t.Fatalf("Expected nil error, got: %v", err)
			}

			var (
				expectMark = 0
				dupes      = make(map[int64]int)
			)

			for i, step := range tc.steps {
				if len(step.expect) > 0 {
					expectMark += 1
				}

				if step.stamp == flushStamp {
					if done := rw.Flush(); done {
						t.Fatalf("Expected false step: %v, got %v", i+1, done)
					}
				} else {
					cnt := dupes[step.stamp]
					dupes[step.stamp] = cnt + 1
					line := fmt.Sprintf("%d.%d", step.stamp, cnt)

					v := rw.Append(LogEntry{Timestamp: step.stamp, Line: line})
					if tc.dmark > 0 {
						if expectMark >= tc.dmark && !v {
							t.Errorf("Expected done to be true on dmark")
						}
					} else if v != false {
						t.Errorf("Expected false step: %v, got %v", i+1, v)
					}
				}

				if len(entries) != len(step.expect) {
					t.Fatalf("Expected %d entries step %d, got %d", len(step.expect), i+1, len(entries))
				}

				dupeCnt := 0
				for j, expect := range step.expect {
					if j > 0 && step.expect[j-1] == expect {
						dupeCnt++
					} else {
						dupeCnt = 0
					}
					line := fmt.Sprintf("%d.%d", expect, dupeCnt)
					if entries[j].Line != line {
						t.Errorf("Expected %s step: %d, got %s", line, i+1, entries[j].Line)
					}
				}

				entries = nil
			}

			// Validate queues were flushed
			if tc.dmark > 0 {
				if rw.clock != 0 {
					t.Errorf("Expected clock is 0, got: %v", rw.clock)
				}
				if !rw.inList.empty() {
					t.Errorf("Expected empty inList")
				}
				if !rw.ooList.empty() {
					t.Errorf("Expected empty ooList")
				}
			}

		})
	}
}

func TestAdvanceClock(t *testing.T) {
	var entries []LogEntry
	cb := func(entry LogEntry) bool {
		entries = append(entries, entry)
		return false
	}
	rw, err := NewReorder(10, cb)
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	rw.Append(LogEntry{Timestamp: 1})
	if rw.clock != 1 {
		t.Fatalf("Expected clock to be reset to 1, got %d", rw.clock)
	}

	if done := rw.AdvanceClock(10); done {
		t.Errorf("Expected false, got %v", done)
	}

	if len(entries) != 0 {
		t.Fatalf("Expected 0 entries, got %d", len(entries))
	}

	if done := rw.AdvanceClock(11); done {
		t.Errorf("Expected false, got %v", done)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entries, got %d", len(entries))
	}

	// Regression on clock should be ignored
	if done := rw.AdvanceClock(3); done {
		t.Errorf("Expected false, got %v", done)
	}

	if rw.clock != 11 {
		t.Fatalf("Expected clock to be 11, got %d", rw.clock)
	}
}

// -----

func BenchmarkInOrder(b *testing.B) {

	cb := func(entry LogEntry) bool {
		return false
	}
	rw, err := NewReorder(10, cb)
	if err != nil {
		b.Fatalf("Expected nil error, got: %v", err)
	}

	for i := 0; i < b.N; i++ {
		rw.Append(LogEntry{Timestamp: int64(i), Line: "benchmark"})
	}
}

func BenchmarkOutOfOrder(b *testing.B) {

	cb := func(entry LogEntry) bool {
		return false
	}

	rw, err := NewReorder(10, cb)
	if err != nil {
		b.Fatalf("Expected nil error, got: %v", err)
	}

	for i := 0; i < b.N; i++ {
		if i > 10 && i%5 == 0 {
			rw.Append(LogEntry{Timestamp: int64(i - 5), Line: "benchmark"})
		}
		rw.Append(LogEntry{Timestamp: int64(i), Line: "benchmark"})
	}
}
