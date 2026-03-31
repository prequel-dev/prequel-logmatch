package match

import (
	"reflect"
	"testing"
)

func TestNewScanLineAndReset(t *testing.T) {
	le := LogEntry{Line: `{"foo":"bar"}`, Timestamp: 123}
	sl := NewScanLine().Reset(le)
	if sl.LogEntry.Line != le.Line {
		t.Errorf("expected line %q, got %q", le.Line, sl.LogEntry.Line)
	}

	if sl.cache != nil {
		t.Errorf("expected cache to be nil on new ScanLine, got %v", sl.cache)
	}

	// Force decoding to populate cache
	_, err := sl.DecodeJson()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Reset with same line (should keep cache)
	sl.Reset(le)
	if sl.cache.ty != decodeJson {
		t.Errorf("expected dtyp decodeJson after reset with same line, got %v", sl.cache.ty)
	}
	if !reflect.DeepEqual(sl.cache.ptr, map[string]any{"foo": "bar"}) {
		t.Errorf("expected dany unchanged after reset with same line")
	}

	// Reset with different line (should clear cache)
	le2 := LogEntry{Line: `{"baz":123}`, Timestamp: 456}
	sl.Reset(le2)
	if sl.cache.ty != decodeNone {
		t.Errorf("expected dtyp decodeNone after reset with new line, got %v", sl.cache.ty)
	}
}

func TestDecodeJson_Invalid(t *testing.T) {
	sl := NewScanLine().ResetLine(0, `not a json`)
	val, err := sl.DecodeJson()
	if err == nil {
		t.Errorf("expected error for invalid json")
	}
	if val != nil {
		t.Errorf("expected nil value for invalid json")
	}
	if sl.cache == nil {
		t.Fatalf("expected cache to be initialized on decode attempt")
	}
	if sl.cache.ty != decodeJson {
		t.Errorf("should cache error result with dtyp decodeJson, got %v", sl.cache.ty)
	}
	if sl.cache.err != err {
		t.Errorf("expected cached error to match returned error")
	}
}

func TestDecodeYaml_SuccessAndCache(t *testing.T) {
	sl := NewScanLine().ResetLine(0, "foo: bar\nnum: 42")

	// First decode
	val, err := sl.DecodeYaml()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m, ok := val.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", val)
	}

	v, ok := m["foo"].(string)
	if !ok || v != "bar" {
		t.Errorf("unexpected value for foo: %v", m["foo"])
	}
	num, ok := m["num"].(uint64)
	if !ok || num != 42 {
		t.Errorf("unexpected value for num: %v", m["num"])
	}

	if sl.cache == nil {
		t.Fatalf("expected cache to be initialized on decode")
	}

	if sl.cache.ty != decodeYaml {
		t.Errorf("expected dtyp decodeYaml, got %v", sl.cache.ty)
	}
	if !reflect.DeepEqual(sl.cache.ptr, m) {
		t.Errorf("expected dany to be cached value")
	}

	// Second decode (should use cache)
	val2, err2 := sl.DecodeYaml()
	if err2 != nil {
		t.Fatalf("unexpected error on cached decode: %v", err2)
	}

	m, ok = val2.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", val2)
	}
	v, ok = m["foo"].(string)
	if !ok || v != "bar" {
		t.Errorf("unexpected value for foo: %v", m["foo"])
	}
}

func TestDecodeYaml_Invalid(t *testing.T) {
	sl := NewScanLine().ResetLine(0, ":\n-")
	val, err := sl.DecodeYaml()
	if err == nil {
		t.Errorf("expected error for invalid yaml")
	}
	if val != nil {
		t.Errorf("expected nil value for invalid yaml")
	}
	if sl.cache == nil {
		t.Fatalf("expected cache to be initialized on decode attempt")
	}
	if sl.cache.ty != decodeYaml {
		t.Errorf("should cache error result with dtyp decodeYaml, got %v", sl.cache.ty)
	}
	if sl.cache.err != err {
		t.Errorf("expected cached error to match returned error")
	}
}

func TestDecodeJsonThenYaml(t *testing.T) {
	sl := NewScanLine().ResetLine(0, `{"foo": "bar"}`)

	// Decode as JSON
	_, err := sl.DecodeJson()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sl.cache == nil {
		t.Fatalf("expected cache to be initialized on decode")
	}
	if sl.cache.ty != decodeJson {
		t.Errorf("expected dtyp decodeJson, got %v", sl.cache.ty)
	}

	// Decode as YAML (should decode again, not use JSON cache)
	_, err = sl.DecodeYaml()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sl.cache.ty != decodeYaml {
		t.Errorf("expected dtyp decodeYaml, got %v", sl.cache.ty)
	}
	if sl.cache.ptr == nil {
		t.Errorf("expected dany to be set after YAML decode")
	}
	if sl.cache.err != nil {
		t.Errorf("expected no error after successful YAML decode, got %v", sl.cache.err)
	}

}

func TestDecodeYamlThenJson(t *testing.T) {
	sl := NewScanLine().ResetLine(0, "foo: bar")

	// Decode as YAML
	valYaml, err := sl.DecodeYaml()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valYaml == nil {
		t.Errorf("expected non-nil value from YAML decode")
	}
	if sl.cache == nil {
		t.Fatalf("expected cache to be initialized on decode")
	}
	if sl.cache.ty != decodeYaml {
		t.Errorf("expected dtyp decodeYaml, got %v", sl.cache.ty)
	}

	// Decode as JSON (should decode again, not use YAML cache, and fail)
	valJson, err := sl.DecodeJson()
	if err == nil {
		t.Errorf("expected error decoding YAML as JSON")
	}
	if valJson != nil {
		t.Errorf("expected nil value for invalid JSON")
	}
	if sl.cache == nil {
		t.Fatalf("expected cache to be initialized on decode attempt")
	}
	if sl.cache.ty != decodeJson {
		t.Errorf("expected dtyp decodeJson, got %v", sl.cache.ty)
	}
	if sl.cache.err != err {
		t.Errorf("expected cached error to match returned error")
	}
}

func TestScanLine_Integration(t *testing.T) {
	// Test full cycle: JSON, YAML, Reset, etc.
	sl := NewScanLine().ResetLine(0, `{"a":1}`)
	_, err := sl.DecodeJson()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sl.Reset(LogEntry{Line: "b: 2"})
	if sl.cache == nil {
		t.Fatalf("expected cache to be initialized on reset")
	}
	if sl.cache.ty != decodeNone || sl.cache.ptr != nil {
		t.Errorf("expected cache cleared after reset")
	}
	_, err = sl.DecodeYaml()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNoDecodeMeansNoCache(t *testing.T) {
	sl := NewScanLine().ResetLine(0, "just a line")
	if sl.cache != nil {
		t.Errorf("expected cache to be nil if no decode attempted")
	}
}
