package match

import "testing"

func makeTestLogs(n int) []LogEntry {
	logs := make([]LogEntry, n)
	for i := range logs {
		logs[i] = LogEntry{}
	}
	return logs
}

func TestHitsPopFront(t *testing.T) {
	h := Hits{
		Cnt:  2,
		Logs: makeTestLogs(4),
	}
	// Each group should be 2 logs
	group := h.PopFront()
	if len(group) != 2 {
		t.Errorf("Expected 2 logs, got %d", len(group))
	}
	if h.Cnt != 1 {
		t.Errorf("Expected Cnt to be 1, got %d", h.Cnt)
	}
	group2 := h.PopFront()
	if len(group2) != 2 {
		t.Errorf("Expected 2 logs, got %d", len(group2))
	}
	if h.Cnt != 0 {
		t.Errorf("Expected Cnt to be 0, got %d", h.Cnt)
	}
	// Should return nil when empty
	group3 := h.PopFront()
	if group3 != nil {
		t.Errorf("Expected nil, got %v", group3)
	}
}

func TestHitsLast(t *testing.T) {
	h := Hits{
		Cnt:  3,
		Logs: makeTestLogs(6),
	}
	last := h.Last()
	if len(last) != 2 {
		t.Errorf("Expected 2 logs in last group, got %d", len(last))
	}
	// Should be same as Index(2)
	idx := h.Index(2)
	if len(idx) != 2 {
		t.Errorf("Expected 2 logs in index group, got %d", len(idx))
	}

}

func TestHitsIndex(t *testing.T) {
	h := Hits{
		Cnt:  4,
		Logs: makeTestLogs(8),
	}
	for i := 0; i < 4; i++ {
		group := h.Index(i)
		if len(group) != 2 {
			t.Errorf("Expected 2 logs in group %d, got %d", i, len(group))
		}
	}
	// Out of bounds
	if h.Index(4) != nil {
		t.Errorf("Expected nil for out of bounds index")
	}
}

func TestHitsIndexNegative(t *testing.T) {
	h := Hits{
		Cnt:  2,
		Logs: makeTestLogs(4),
	}
	if h.Index(-1) != nil {
		t.Errorf("Expected nil for negative index")
	}
}

func TestHitsIndexProps(t *testing.T) {
	h := Hits{
		Cnt:  2,
		Logs: makeTestLogs(4),
		Props: map[PropKey]any{
			{Idx: 0, Key: "key1"}: "value1",
			{Idx: 0, Key: "key2"}: "value2",
			{Idx: 1, Key: "key1"}: "value3",
			{Idx: 1, Key: "key3"}: "value4",
		},
	}

	// Test index 0 props
	props0 := h.IndexProps(0)
	if len(props0) != 2 {
		t.Errorf("Expected 2 props for index 0, got %d", len(props0))
	}
	if props0["key1"] != "value1" {
		t.Errorf("Expected 'value1' for key1, got %v", props0["key1"])
	}
	if props0["key2"] != "value2" {
		t.Errorf("Expected 'value2' for key2, got %v", props0["key2"])
	}

	// Test index 1 props
	props1 := h.IndexProps(1)
	if len(props1) != 2 {
		t.Errorf("Expected 2 props for index 1, got %d", len(props1))
	}
	if props1["key1"] != "value3" {
		t.Errorf("Expected 'value3' for key1, got %v", props1["key1"])
	}
	if props1["key3"] != "value4" {
		t.Errorf("Expected 'value4' for key3, got %v", props1["key3"])
	}

	// Test non-existent index
	props2 := h.IndexProps(2)
	if props2 != nil {
		t.Errorf("Expected nil for non-existent index, got %v", props2)
	}
}

func TestHitsIndexPropsNilProps(t *testing.T) {
	h := Hits{
		Cnt:   2,
		Logs:  makeTestLogs(4),
		Props: nil, // No props
	}

	props := h.IndexProps(0)
	if props != nil {
		t.Errorf("Expected nil when Props is nil, got %v", props)
	}
}

func TestHitsIndexPropsEmptyProps(t *testing.T) {
	h := Hits{
		Cnt:   2,
		Logs:  makeTestLogs(4),
		Props: make(map[PropKey]any), // Empty props
	}

	props := h.IndexProps(0)
	if props != nil {
		t.Errorf("Expected nil when no props for index, got %v", props)
	}
}

func TestHitsPopFrontZeroCount(t *testing.T) {
	h := Hits{
		Cnt:  0,
		Logs: makeTestLogs(0),
	}

	result := h.PopFront()
	if result != nil {
		t.Errorf("Expected nil when Cnt is 0, got %v", result)
	}
}

func TestHitsPopFrontNegativeCount(t *testing.T) {
	h := Hits{
		Cnt:  -1,
		Logs: makeTestLogs(0),
	}

	result := h.PopFront()
	if result != nil {
		t.Errorf("Expected nil when Cnt is negative, got %v", result)
	}
}

func TestHitsLastEmptyHits(t *testing.T) {
	h := Hits{
		Cnt:  0,
		Logs: makeTestLogs(0),
	}

	result := h.Last()
	if result != nil {
		t.Errorf("Expected nil when Cnt is 0, got %v", result)
	}
}

func TestHitsIndexEdgeCases(t *testing.T) {
	h := Hits{
		Cnt:  3,
		Logs: makeTestLogs(9),
	}

	// Test first index
	first := h.Index(0)
	if len(first) != 3 {
		t.Errorf("Expected 3 logs for first index, got %d", len(first))
	}

	// Test middle index
	middle := h.Index(1)
	if len(middle) != 3 {
		t.Errorf("Expected 3 logs for middle index, got %d", len(middle))
	}

	// Test last valid index
	last := h.Index(2)
	if len(last) != 3 {
		t.Errorf("Expected 3 logs for last index, got %d", len(last))
	}

	// Test boundary conditions
	if h.Index(-1) != nil {
		t.Errorf("Expected nil for index -1")
	}
	if h.Index(3) != nil {
		t.Errorf("Expected nil for index 3 (out of bounds)")
	}
}

func TestHitsUnevenLogDistribution(t *testing.T) {
	// Test case where logs don't divide evenly
	h := Hits{
		Cnt:  3,
		Logs: makeTestLogs(8), // 8 logs, 3 groups = 2 logs per group (with remainder)
	}

	for i := 0; i < 3; i++ {
		group := h.Index(i)
		if len(group) != 2 { // 8/3 = 2 (integer division)
			t.Errorf("Expected 2 logs in group %d, got %d", i, len(group))
		}
	}
}

func TestPropKeyStruct(t *testing.T) {
	// Test that PropKey can be used as a map key
	props := make(map[PropKey]any)
	key1 := PropKey{Idx: 0, Key: "test"}
	key2 := PropKey{Idx: 1, Key: "test"}
	key3 := PropKey{Idx: 0, Key: "test"} // Same as key1

	props[key1] = "value1"
	props[key2] = "value2"
	props[key3] = "value3" // Should overwrite value1

	if len(props) != 2 {
		t.Errorf("Expected 2 unique keys, got %d", len(props))
	}
	if props[key1] != "value3" {
		t.Errorf("Expected key1 to be overwritten with 'value3', got %v", props[key1])
	}
}
