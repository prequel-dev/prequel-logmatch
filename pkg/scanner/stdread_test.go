package scanner

import (
	"testing"
)

func TestNewStdReadScanAndScanResult(t *testing.T) {
	sr := NewStdReadScan(1024)
	if sr.maxSz != 1024 {
		t.Errorf("NewStdReadScan did not set maxSz")
	}
	if len(sr.logs) != 0 {
		t.Errorf("NewStdReadScan logs should be empty")
	}

	entry := LogEntry{Line: "foo", Stream: "stdout", Timestamp: 1}
	clipped := sr.Scan(entry)
	if clipped {
		t.Errorf("Scan should not clip for small entry")
	}
	res := sr.Result()
	if res.Sz == 0 {
		t.Errorf("Result Sz should be > 0 after scan")
	}
	if res.Clip {
		t.Errorf("Result Clip should be false for small input")
	}
	if len(res.Logs) != 1 {
		t.Errorf("Result Logs should have 1 entry")
	}
}

func TestStdReadScanClip(t *testing.T) {
	sr := NewStdReadScan(10)
	entry := LogEntry{Line: "foofoofoofoofoofoo", Stream: "stdout", Timestamp: 1}
	clipped := sr.Scan(entry)
	if !clipped {
		t.Errorf("Scan should clip for large entry")
	}
	res := sr.Result()
	if !res.Clip {
		t.Errorf("Result Clip should be true for large input")
	}
}

func TestStdReadScanBind(t *testing.T) {
	sr := NewStdReadScan(1024)
	bind := sr.Bind()
	entry := LogEntry{Line: "foo", Stream: "stdout", Timestamp: 1}
	bind(entry)
	res := sr.Result()
	if len(res.Logs) != 1 {
		t.Errorf("Bind should append log entry")
	}
}
