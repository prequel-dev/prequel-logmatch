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
