package match

import (
	"encoding/json"

	"github.com/goccy/go-yaml"
)

type decodeT int

const (
	decodeNone decodeT = iota
	decodeJson
	decodeYaml
)

// ScanLine is a wrapper around LogEntry that provides caching for decoded JSON and YAML data.
// It is a replacement for the memoized struct that was previously used in the Matcher implementation,
// and is designed to be used across multiple matchers without needing to duplicate the memoization logic in each matcher.
// The cache is simple: it stores the type of the last decode (JSON or YAML), the decoded value, and any error that occurred during decoding.
// Matchers that switch between JSON and YAML decoding on the same line will cause cache invalidation.
// In general, matchers should stick to one decode type per line for best performance.

type ScanLine struct {
	LogEntry
	cache *cacheT // Allocate lazily only if needed; TODO: Consider making this a weak ptr.
}

type cacheT struct {
	ty  decodeT
	ptr any
	err error
}

func NewScanLine() *ScanLine {
	return &ScanLine{}
}

// If cache is available and the line is the same, we can reuse the cached value.
// If the line has changed, we need to clear the cache.
func (s *ScanLine) _maybeClear(line string) {
	switch {
	case s.cache == nil:
		// Fall through;  nothing to clear
	case s.LogEntry.Line != line:
		// Clear the cache if the line has changed
		s.cache.ty = decodeNone
		s.cache.ptr = nil
		s.cache.err = nil
	}
}

func (s *ScanLine) Reset(e LogEntry) *ScanLine {
	s._maybeClear(e.Line)
	s.LogEntry = e
	return s
}

func (s *ScanLine) ResetLine(ts int64, line string) *ScanLine {
	return s.Reset(LogEntry{Line: line, Timestamp: ts})
}

func (s *ScanLine) DecodeJson() (any, error) {
	if s.cache != nil && s.cache.ty == decodeJson {
		return s.cache.ptr, s.cache.err
	}
	return s._decode(decodeJson, json.Unmarshal)
}

func (s *ScanLine) DecodeYaml() (any, error) {
	if s.cache != nil && s.cache.ty == decodeYaml {
		return s.cache.ptr, s.cache.err
	}
	return s._decode(decodeYaml, yaml.Unmarshal)
}

func (s *ScanLine) _decode(ty decodeT, unmarshal func([]byte, interface{}) error) (any, error) {

	if s.cache == nil {
		s.cache = &cacheT{ty: ty}
	} else {
		s.cache.ty = ty
		s.cache.err = nil
	}

	var dany any
	if err := unmarshal([]byte(s.Line), &dany); err != nil {
		s.cache.ptr = nil
		s.cache.err = err
		return nil, err
	}

	s.cache.ptr = dany
	return dany, nil
}
