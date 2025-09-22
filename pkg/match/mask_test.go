package match

import (
	"testing"
)

func TestBitMaskT(t *testing.T) {
	var m bitMaskT

	// Test Set
	m.Set(0)
	if !m.IsSet(0) {
		t.Errorf("Expected slot 0 to be set")
	}
	m.Set(3)
	if !m.IsSet(3) {
		t.Errorf("Expected slot 3 to be set")
	}
	if m.IsSet(2) {
		t.Errorf("Expected slot 2 to be unset")
	}

	// Test Clr
	m.Clr(0)
	if m.IsSet(0) {
		t.Errorf("Expected slot 0 to be cleared")
	}
	if !m.IsSet(3) {
		t.Errorf("Expected slot 3 to remain set")
	}

	// Test Reset
	m.Reset()
	if !m.Zeros() {
		t.Errorf("Expected mask to be zero after reset")
	}

	// Test FirstN
	m.Set(0)
	m.Set(1)
	m.Set(2)
	if !m.FirstN(3) {
		t.Errorf("Expected first 3 bits to be set")
	}
	if m.FirstN(4) {
		t.Errorf("Expected first 4 bits not to be set")
	}

	// Test Zeros
	m.Reset()
	if !m.Zeros() {
		t.Errorf("Expected Zeros to return true after reset")
	}
	m.Set(5)
	if m.Zeros() {
		t.Errorf("Expected Zeros to return false when a bit is set")
	}
}
