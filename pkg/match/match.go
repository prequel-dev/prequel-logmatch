package match

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/prequel-dev/prequel-logmatch/pkg/entry"

	"github.com/itchyny/gojq"
	"github.com/rs/zerolog/log"
)

var (
	ErrTermType    = errors.New("unknown term type")
	ErrTermEmpty   = errors.New("empty term")
	ErrTermCompile = errors.New("term compile error")
)

type Matcher interface {
	Eval(int64) Hits
	Scan(*ScanLine) Hits
	GarbageCollect(int64)
}

type LogEntry = entry.LogEntry

type TermTypeT int

const (
	TermRaw TermTypeT = iota
	TermRegex
	TermJqJson
	TermJqYaml
)

const (
	termNameRaw     = "raw"
	termNameRegex   = "regex"
	termNameJqJson  = "jqJson"
	termNameJqYaml  = "jqYaml"
	termNameUnknown = "unknown"
)

func (t TermTypeT) String() string {
	switch t {
	case TermRaw:
		return termNameRaw
	case TermJqJson:
		return termNameJqJson
	case TermJqYaml:
		return termNameJqYaml
	case TermRegex:
		return termNameRegex
	default:
		return termNameUnknown
	}
}

type TermT struct {
	Type  TermTypeT
	Value string
}

type MatchFunc func(*ScanLine) bool

func (tt TermT) NewMatcher() (m MatchFunc, err error) {

	if tt.Value == "" {
		err = ErrTermEmpty
		return
	}

	switch tt.Type {
	case TermJqJson, TermJqYaml:
		if m, err = makeJqMatch(tt); err != nil {
			err = fmt.Errorf("%w type:'%s' value:'%s': %w", ErrTermCompile, tt.Type.String(), tt.Value, err)
		}
	case TermRegex:
		if m, err = makeRegexMatch(tt.Value); err != nil {
			err = fmt.Errorf("%w type:'%s' value:'%s': %w", ErrTermCompile, tt.Type.String(), tt.Value, err)
		}
	case TermRaw:
		m = makeRawMatch(tt.Value)
	default:
		err = ErrTermType
	}

	return
}

func IsRegex(v string) bool {
	return regexp.QuoteMeta(v) != v
}

func makeRawMatch(s string) MatchFunc {
	return func(e *ScanLine) bool {
		return strings.Contains(e.Line, s)
	}
}

func makeRegexMatch(term string) (MatchFunc, error) {
	exp, err := regexp.Compile(term)
	if err != nil {
		return nil, err
	}

	return func(e *ScanLine) bool {
		return exp.MatchString(e.Line)
	}, nil
}

func jsonUnmarshalThunk(e *ScanLine) (any, error) {
	return e.DecodeJson()
}

func yamlUnmarshalThunk(e *ScanLine) (any, error) {
	return e.DecodeYaml()
}

func NewJqJson(term string) (MatchFunc, error) {

	query, err := gojq.Parse(term)
	if err != nil {
		return nil, fmt.Errorf("%w: parse fail: %w", ErrTermCompile, err)
	}

	code, err := gojq.Compile(query)
	if err != nil {
		return nil, fmt.Errorf("%w: compile fail: %w", ErrTermCompile, err)
	}

	return _makeJqMatch(term, code, jsonUnmarshalThunk), nil
}

func makeJqMatch(term TermT) (MatchFunc, error) {
	var unmarshal unmarshalFuncT

	switch term.Type {
	case TermJqJson:
		unmarshal = jsonUnmarshalThunk
	case TermJqYaml:
		unmarshal = yamlUnmarshalThunk
	default:
		return nil, errors.New("unknown jq format")
	}

	query, err := gojq.Parse(term.Value)
	if err != nil {
		return nil, err
	}

	code, err := gojq.Compile(query)
	if err != nil {
		return nil, err
	}

	return _makeJqMatch(term.Value, code, unmarshal), nil
}

type unmarshalFuncT func(e *ScanLine) (any, error)

func _makeJqMatch(term string, code *gojq.Code, unmarshal unmarshalFuncT) MatchFunc {
	return func(e *ScanLine) (match bool) {
		// Avoid unnecessary allocation on the cast
		var (
			err error
			v   any
		)

		// Unmarshal the line (JSON or YAML) into an interface{} for gojq to consume.
		// The ScanLine will cache the result, so this is only expensive on the first call for a given line.
		if v, err = unmarshal(e); err != nil {
			log.Debug().Err(err).Str("line", e.Line).Msg("Fail parse log line")
			return false
		}
		iter := code.Run(v)
		for {
			res, ok := iter.Next()
			if !ok {
				break
			}
			if err, ok := res.(error); ok {
				if err, ok := err.(*gojq.HaltError); ok && err.Value() == nil {
					break
				}
				log.Debug().Err(err).
					Str("line", e.Line).
					Str("term", term).
					Msg("Fail jq query")
				match = false
				break
			}

			if res != nil {
				if v, ok := res.(bool); ok {
					if v {
						match = true
					}
				} else {
					match = true
				}
			}
		}

		return
	}
}
