package scanner

import (
	"regexp"
	"testing"
)

func TestMakeInvert(t *testing.T) {
	exp := regexp.MustCompile("foo")
	fn := makeInvert(exp)
	if _, ok := fn("foo"); ok {
		t.Errorf("makeInvert should return false for matching string")
	}
	if _, ok := fn("bar"); !ok {
		t.Errorf("makeInvert should return true for non-matching string")
	}
}

func TestMakeFilter(t *testing.T) {
	exp := regexp.MustCompile("foo")
	fn := makeFilter(exp)
	if _, ok := fn("foo"); !ok {
		t.Errorf("makeFilter should return true for matching string")
	}
	if _, ok := fn("bar"); ok {
		t.Errorf("makeFilter should return false for non-matching string")
	}
}

func TestMakeFilterEnrich(t *testing.T) {
	exp := regexp.MustCompile("foo")
	fn := makeFilterEnrich(exp)
	hits, ok := fn("foofoo")
	if !ok || len(hits) != 2 {
		t.Errorf("makeFilterEnrich should find two matches")
	}
	hits, ok = fn("bar")
	if ok || hits != nil {
		t.Errorf("makeFilterEnrich should return nil and false for no match")
	}
}

func TestMakeJump(t *testing.T) {
	exp := regexp.MustCompile("foo")
	fn := makeJump(exp)
	if _, ok := fn("bar"); ok {
		t.Errorf("makeJump should return false before match")
	}
	if _, ok := fn("foo"); !ok {
		t.Errorf("makeJump should return true after match")
	}
	if _, ok := fn("bar"); !ok {
		t.Errorf("makeJump should return true after match")
	}
}

func TestMakeJumpEnrich(t *testing.T) {
	exp := regexp.MustCompile("foo")
	fn := makeJumpEnrich(exp)
	hits, ok := fn("bar")
	if ok || hits != nil {
		t.Errorf("makeJumpEnrich should return nil and false before match")
	}
	hits, ok = fn("foo")
	if !ok || len(hits) != 1 {
		t.Errorf("makeJumpEnrich should return hits and true after match")
	}
	hits, ok = fn("bar")
	if !ok {
		t.Errorf("makeJumpEnrich should return true after match")
	}
}

func TestMakeEnrich(t *testing.T) {
	exp := regexp.MustCompile("foo")
	fn := makeEnrich(exp)
	hits, ok := fn("foofoo")
	if !ok || len(hits) != 2 {
		t.Errorf("makeEnrich should find two matches")
	}
}

func TestMakeFunc(t *testing.T) {
	exp := ExprT{RegEx: regexp.MustCompile("foo"), Mode: ModeEnrich}
	fn := makeFunc(exp, false)
	hits, ok := fn("foofoo")
	if !ok || len(hits) != 2 {
		t.Errorf("makeFunc ModeEnrich failed")
	}

	exp.Mode = ModeInvert
	fn = makeFunc(exp, false)
	if _, ok := fn("foo"); ok {
		t.Errorf("makeFunc ModeInvert failed")
	}

	exp.Mode = ModeJump
	fn = makeFunc(exp, false)
	if _, ok := fn("foo"); !ok {
		t.Errorf("makeFunc ModeJump failed")
	}

	exp.Mode = ModeFilter
	fn = makeFunc(exp, false)
	if _, ok := fn("foo"); !ok {
		t.Errorf("makeFunc ModeFilter failed")
	}
}

func TestNormalizeAndDedupe(t *testing.T) {
	exp1 := ExprT{RegEx: regexp.MustCompile("foo"), Mode: ModeEnrich}
	exp2 := ExprT{RegEx: regexp.MustCompile("foo"), Mode: ModeFilter}
	exp3 := ExprT{RegEx: regexp.MustCompile("foo"), Mode: ModeJump}
	exp4 := ExprT{RegEx: regexp.MustCompile("foo"), Mode: ModeInvert}

	mfuncs := normalize([]ExprT{exp1, exp2, exp3, exp4})
	if len(mfuncs) != 4 {
		t.Errorf("normalize should return 4 functions for 4 modes")
	}

	deduped := dedupe([]ExprT{exp1, exp2, exp3})
	if len(deduped) != 1 {
		t.Errorf("dedupe should optimize to 1 function for enrich, filter, jump")
	}
}

func TestNewMatchScanAndBindResult(t *testing.T) {
	expr := ExprT{RegEx: regexp.MustCompile("foo"), Mode: ModeEnrich}
	ms := NewMatchScan(1024, 0, []ExprT{expr})
	if ms.maxSz != 1024 {
		t.Errorf("NewMatchScan did not set maxSz")
	}
	if ms.flags != 0 {
		t.Errorf("NewMatchScan did not set flags")
	}
	if len(ms.exprs) != 1 {
		t.Errorf("NewMatchScan did not set exprs")
	}

	bind := ms.Bind()
	logEntry := LogEntry{Line: "foofoo", Stream: "stdout", Timestamp: 1}
	bind(logEntry)
	res := ms.Result()
	if res.Sz == 0 {
		t.Errorf("Result Sz should be > 0 after scan")
	}
	if res.Clip {
		t.Errorf("Result Clip should be false for small input")
	}
	if len(res.Logs) != 1 {
		t.Errorf("Result Logs should have 1 entry")
	}
	if len(res.Logs[0].Matches) != 2 {
		t.Errorf("Matches should have 2 hits for 'foofoo'")
	}
}

func TestMatchScanClip(t *testing.T) {
	expr := ExprT{RegEx: regexp.MustCompile("foo"), Mode: ModeEnrich}
	ms := NewMatchScan(10, 0, []ExprT{expr})
	bind := ms.Bind()
	logEntry := LogEntry{Line: "foofoofoofoofoofoo", Stream: "stdout", Timestamp: 1}
	bind(logEntry)
	res := ms.Result()
	if !res.Clip {
		t.Errorf("Result Clip should be true for large input")
	}
}

func TestMatchScanForceUTF16(t *testing.T) {
	expr := ExprT{RegEx: regexp.MustCompile("foo"), Mode: ModeEnrich}
	ms := NewMatchScan(1024, MatchForceUTF16, []ExprT{expr})
	bind := ms.Bind()
	logEntry := LogEntry{Line: "foofoo", Stream: "stdout", Timestamp: 1}
	bind(logEntry)
	res := ms.Result()
	if len(res.Logs) == 0 || len(res.Logs[0].Matches) != 2 {
		t.Errorf("Matches should have 2 hits for 'foofoo'")
	}
	// Check that hits are in UTF16 format (should be int slices)
	for _, hit := range res.Logs[0].Matches {
		if len(hit) != 2 {
			t.Errorf("UTF16 hit should have 2 elements")
		}
	}
}

func TestMatchScanMultipleExprOrder(t *testing.T) {
	exprs := []ExprT{
		{RegEx: regexp.MustCompile("foo"), Mode: ModeInvert},
		{RegEx: regexp.MustCompile("bar"), Mode: ModeFilter},
		{RegEx: regexp.MustCompile("baz"), Mode: ModeJump},
		{RegEx: regexp.MustCompile("qux"), Mode: ModeEnrich},
	}
	ms := NewMatchScan(1024, 0, exprs)
	bind := ms.Bind()
	// Should execute in order: Invert, Filter, Jump, Enrich
	// Only 'foo' should fail Invert, others should pass
	logEntry := LogEntry{Line: "barbazqux", Stream: "stdout", Timestamp: 1}
	ok := bind(logEntry)
	res := ms.Result()
	if ok {
		t.Errorf("Bind should return false, not clipped")
	}
	if len(res.Logs) != 1 {
		t.Errorf("Expected 1 log entry")
	}
	// Validate that matches are only from Enrich (qux)
	if len(res.Logs[0].Matches) != 1 {
		t.Errorf("Expected 1 match from Enrich for 'qux'")
	}
	if res.Logs[0].Matches[0][0] != 6 || res.Logs[0].Matches[0][1] != 9 {
		t.Errorf("Expected match indices for 'qux' at 6,9")
	}
}

func TestDedupeExprTValues(t *testing.T) {
	// Example ExprT values with duplicate modes and regex
	r := regexp.MustCompile("foo")
	exprs := []ExprT{
		{RegEx: r, Mode: ModeEnrich},
		{RegEx: r, Mode: ModeEnrich},
		{RegEx: r, Mode: ModeFilter},
		{RegEx: r, Mode: ModeFilter},
		{RegEx: r, Mode: ModeJump},
		{RegEx: r, Mode: ModeJump},
		{RegEx: r, Mode: ModeInvert},
		{RegEx: r, Mode: ModeInvert},
	}

	deduped := dedupe(exprs)

	// Check that deduped contains only unique mode values
	seen := map[ModeT]bool{}
	for _, v := range deduped {
		if seen[v.exp.Mode] {
			t.Errorf("Duplicate mode %v found in deduped slice", v.exp.Mode)
		}
		seen[v.exp.Mode] = true
	}

	// Check expected length (should be 4 for all modes)
	if len(deduped) != 4 {
		t.Errorf("Expected 4 unique modes, got %d", len(deduped))
	}
}

func TestDedupeEnrichAndJumpOptimized(t *testing.T) {
	r := regexp.MustCompile("foo")
	exprs := []ExprT{
		{RegEx: r, Mode: ModeEnrich},
		{RegEx: r, Mode: ModeJump},
	}
	deduped := dedupe(exprs)
	if len(deduped) != 1 {
		t.Errorf("Expected 1 optimized deduped value, got %d", len(deduped))
	}
	if deduped[0].exp.Mode != ModeJump || !deduped[0].enrich {
		t.Errorf("Expected ModeJump with enrich=true, got mode=%v enrich=%v", deduped[0].exp.Mode, deduped[0].enrich)
	}
}

func TestDedupeEnrichAndFilterOptimized(t *testing.T) {
	r := regexp.MustCompile("foo")
	exprs := []ExprT{
		{RegEx: r, Mode: ModeEnrich},
		{RegEx: r, Mode: ModeFilter},
	}
	deduped := dedupe(exprs)
	if len(deduped) != 1 {
		t.Errorf("Expected 1 optimized deduped value, got %d", len(deduped))
	}
	if deduped[0].exp.Mode != ModeFilter || !deduped[0].enrich {
		t.Errorf("Expected ModeFilter with enrich=true, got mode=%v enrich=%v", deduped[0].exp.Mode, deduped[0].enrich)
	}
}

func TestDedupeFilterAndJumpOptimized(t *testing.T) {
	r := regexp.MustCompile("foo")
	exprs := []ExprT{
		{RegEx: r, Mode: ModeFilter},
		{RegEx: r, Mode: ModeJump},
	}
	deduped := dedupe(exprs)
	if len(deduped) != 1 {
		t.Errorf("Expected 1 optimized deduped value, got %d", len(deduped))
	}
	if deduped[0].exp.Mode != ModeFilter || deduped[0].enrich {
		t.Errorf("Expected ModeFilter with enrich=false, got mode=%v enrich=%v", deduped[0].exp.Mode, deduped[0].enrich)
	}
}

func TestOptimizedMatchModesLogicalCorrectness(t *testing.T) {
	reg := regexp.MustCompile("foo")

	// Enrich + Jump -> Jump with enrich=true
	exprs := []ExprT{
		{RegEx: reg, Mode: ModeEnrich},
		{RegEx: reg, Mode: ModeJump},
	}
	deduped := dedupe(exprs)
	if len(deduped) != 1 {
		t.Fatalf("Expected 1 optimized result, got %d", len(deduped))
	}
	mfunc := makeFunc(deduped[0].exp, deduped[0].enrich)
	// Should match after first match
	_, ok := mfunc("foo")
	if !ok {
		t.Errorf("Jump+enrich should match on 'foo'")
	}
	_, ok = mfunc("foo")
	if !ok {
		t.Errorf("Jump+enrich should remain matched on subsequent 'foo'")
	}

	// Enrich + Filter -> Filter with enrich=true
	exprs = []ExprT{
		{RegEx: reg, Mode: ModeEnrich},
		{RegEx: reg, Mode: ModeFilter},
	}
	deduped = dedupe(exprs)
	if len(deduped) != 1 {
		t.Fatalf("Expected 1 optimized result, got %d", len(deduped))
	}
	mfunc = makeFunc(deduped[0].exp, deduped[0].enrich)
	// Should match and return hits
	hits, ok := mfunc("foofoo")
	if !ok || len(hits) != 2 {
		t.Errorf("Filter+enrich should return 2 hits for 'foofoo', got %v", hits)
	}

	// Filter + Jump -> Filter with enrich=false
	exprs = []ExprT{
		{RegEx: reg, Mode: ModeFilter},
		{RegEx: reg, Mode: ModeJump},
	}
	deduped = dedupe(exprs)
	if len(deduped) != 1 {
		t.Fatalf("Expected 1 optimized result, got %d", len(deduped))
	}
	mfunc = makeFunc(deduped[0].exp, deduped[0].enrich)
	// Should match and return nil hits
	hits, ok = mfunc("foofoo")
	if !ok || hits != nil {
		t.Errorf("Filter (no enrich) should match but return nil hits for 'foofoo'")
	}
}
