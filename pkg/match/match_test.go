package match

import (
	"errors"
	"testing"
)

func TestMatchJson(t *testing.T) {

	tt := TermT{
		Type:  TermJqJson,
		Value: `select(.shrubbery == "apple")`,
	}

	m, err := tt.NewMatcher()
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`{"shrubbery":"apple"}`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`{"nope":"apple"}`) {
		t.Errorf("Expected no match, got match.")
	}

	// Error path
	if m(`not json`) {
		t.Errorf("Expected no match, got match.")
	}
}

func TestMatchJsonHalt(t *testing.T) {

	tt := TermT{
		Type:  TermJqJson,
		Value: `halt_error("bad input")`,
	}

	m, err := tt.NewMatcher()
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Fail path
	if m(`{"a":"shrubbery"}`) {
		t.Errorf("Expected no match, got match.")
	}

}

func TestNewJqJson(t *testing.T) {

	m, err := NewJqJson(`select(.shrubbery == "apple")`)
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`{"shrubbery":"apple"}`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`{"nope":"apple"}`) {
		t.Errorf("Expected no match, got match.")
	}
}

func TestNewJqJsonBadTerm(t *testing.T) {

	// Fail the parse test
	_, err := NewJqJson(".[] |")
	if !errors.Is(err, ErrTermCompile) {
		t.Fatalf("Expected error, got :%v", err)
	}

	// Fail the compile test
	_, err = NewJqJson(`badterm`)
	if !errors.Is(err, ErrTermCompile) {
		t.Fatalf("Expected error, got :%v", err)
	}
}

func TestJqJsonBadLine(t *testing.T) {

	mFunc, err := NewJqJson(`select(.shrubbery == "apple")`)
	if err != nil {
		t.Fatalf("Expected nil, got :%v", err)
	}

	badLine := `apple, but not json`

	// Execute path for coverage
	if mFunc(badLine) {
		t.Errorf("Expected no match, got match.")
	}

	// Run it again, should hit dupe cache
	if mFunc(badLine) {
		t.Errorf("Expected no match, got match.")
	}
}

func TestMatchJsonString(t *testing.T) {
	tt := TermT{
		Type:  TermJqJson,
		Value: `select(.shrubbery == "apple")`,
	}

	m, err := tt.NewMatcher()
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`{"shrubbery":"apple"}`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`{"shrubbery":"xapple"}`) {
		t.Errorf("Expected no match, got match.")
	}

}

func TestMatchJsonRegex(t *testing.T) {
	tt := TermT{
		Type:  TermJqJson,
		Value: `.shrubbery | test("^a.")`,
	}

	m, err := tt.NewMatcher()
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`{"shrubbery":"apple"}`) {
		t.Errorf("Expected match, got fail.")
	}

	if !m(`{"shrubbery":"applex"}`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`{"shrubbery":"banana"}`) {
		t.Errorf("Expected no match, got match.")
	}
}

func TestMatchYaml(t *testing.T) {
	tt := TermT{
		Type:  TermJqYaml,
		Value: `.shrubbery`,
	}

	m, err := tt.NewMatcher()
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`shrubbery: apple`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`nope: apple`) {
		t.Errorf("Expected no match, got match.")
	}
}

func TestMatchRegex(t *testing.T) {
	tt := TermT{
		Type:  TermRegex,
		Value: `[A-Z]+`,
	}

	m, err := tt.NewMatcher()
	if err != nil {
		t.Fatalf("Expected nil error, got: %v", err)
	}

	// Happy path
	if !m(`HELLO`) {
		t.Errorf("Expected match, got fail.")
	}

	// Sad path
	if m(`hello`) {
		t.Errorf("Expected no match, got match.")
	}
}

func TestIsRegex(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{input: "apple", expected: false},
		{input: "a.*e", expected: true},
		{input: "[A-Z]+", expected: true},
		{input: "banana?", expected: true},
		{input: "cherry", expected: false},
	}

	for _, test := range tests {
		result := IsRegex(test.input)
		if result != test.expected {
			t.Errorf("IsRegex(%q) = %v; want %v", test.input, result, test.expected)
		}
	}
}

func TestTermString(t *testing.T) {
	terms := []struct {
		input    TermTypeT
		expected string
	}{
		{TermRaw, termNameRaw},
		{TermRegex, termNameRegex},
		{TermJqJson, termNameJqJson},
		{TermJqYaml, termNameJqYaml},
		{TermTypeT(999), termNameUnknown},
	}

	for _, term := range terms {
		if term.input.String() != term.expected {
			t.Errorf("Expected %q, got %q", term.expected, term.input.String())
		}
	}
}

func TestJqYamlBadLine(t *testing.T) {

	mFunc, err := makeJqMatch(TermT{Type: TermJqYaml, Value: `select(.shrubbery == "apple")`})
	if err != nil {
		t.Fatalf("Expected nil, got :%v", err)
	}

	badLine := `apple, but not yaml`

	// Execute path for coverage
	if mFunc(badLine) {
		t.Errorf("Expected no match, got match.")
	}

	// Run it again, should hit dupe cache
	if mFunc(badLine) {
		t.Errorf("Expected no match, got match.")
	}
}

func TestTermBadJqExpression(t *testing.T) {
	tt := TermT{
		Type:  TermJqYaml,
		Value: `invalid jq`,
	}
	_, err := tt.NewMatcher()
	if !errors.Is(err, ErrTermCompile) {
		t.Fatalf("Expected error, got nil")
	}
}

func TestTermBadRegexExpression(t *testing.T) {
	tt := TermT{
		Type:  TermRegex,
		Value: `[A-Z`,
	}
	_, err := tt.NewMatcher()
	if !errors.Is(err, ErrTermCompile) {
		t.Fatalf("Expected error, got nil")
	}
}

func TestTermBadType(t *testing.T) {
	tt := TermT{
		Type:  TermTypeT(999),
		Value: `some value`,
	}
	_, err := tt.NewMatcher()
	if !errors.Is(err, ErrTermType) {
		t.Fatalf("Expected error, got nil")
	}
}

var jsonData = `
{
  "widget": {
    "debug": "on",
    "window": {
      "title": "Sample Konfabulator Widget",
      "name": "main_window",
      "width": 500,
      "height": 500
    },
    "image": { 
      "src": "Images/Sun.png",
      "hOffset": 250,
      "vOffset": 250,
      "alignment": "center"
    },
    "text": {
      "data": "Click Here",
      "size": 36,
      "style": "bold",
      "vOffset": 100,
      "alignment": "center",
      "onMouseUp": "sun1.opacity = (sun1.opacity / 100) * 90;"
    }
  }
} `

func BenchmarkMatchJson(b *testing.B) {
	var (
		tt1 = TermT{
			Type:  TermJqJson,
			Value: `.widget.window.name`,
		}
		tt2 = TermT{
			Type:  TermJqJson,
			Value: `.widget.image.hOffset`,
		}
		tt3 = TermT{
			Type:  TermJqJson,
			Value: `.widget.text.onMouseUp`,
		}

		m1, _ = tt1.NewMatcher()
		m2, _ = tt2.NewMatcher()
		m3, _ = tt3.NewMatcher()
	)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m1(jsonData)
		m2(jsonData)
		m3(jsonData)
	}

}

func TestMakeJqMatchFailureCases(t *testing.T) {
	tests := []struct {
		name        string
		term        TermT
		expectError bool
		errorMsg    string
	}{
		{
			name: "InvalidJqSyntax",
			term: TermT{
				Type:  TermJqJson,
				Value: "invalid jq syntax ][",
			},
			expectError: true,
		},
		{
			name: "UnknownJqFormat",
			term: TermT{
				Type:  TermTypeT(999), // Unknown type
				Value: ".field",
			},
			expectError: true,
			errorMsg:    "unknown jq format",
		},
		{
			name: "InvalidJqFilter",
			term: TermT{
				Type:  TermJqJson,
				Value: ".field | unknown_function",
			},
			expectError: true,
		},
		{
			name: "EmptyJqQuery",
			term: TermT{
				Type:  TermJqJson,
				Value: "",
			},
			expectError: true,
		},
		{
			name: "MalformedJqQuery",
			term: TermT{
				Type:  TermJqYaml,
				Value: ".[unclosed",
			},
			expectError: true,
		},
		{
			name: "ValidJqQuery",
			term: TermT{
				Type:  TermJqJson,
				Value: ".field",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher, err := makeJqMatch(tt.term)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for term %+v, but got nil", tt.term)
				}
				if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("Expected error message '%s', got '%s'", tt.errorMsg, err.Error())
				}
				if matcher != nil {
					t.Error("Expected nil matcher when error occurs")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error for valid term, got %v", err)
				}
				if matcher == nil {
					t.Error("Expected valid matcher for valid term")
				}
			}
		})
	}
}

func TestJqMatchRuntimeFailures(t *testing.T) {
	tests := []struct {
		name     string
		term     TermT
		input    string
		expected bool
	}{
		{
			name: "MalformedJSON",
			term: TermT{
				Type:  TermJqJson,
				Value: ".field",
			},
			input:    "{malformed json",
			expected: false,
		},
		{
			name: "MalformedYAML",
			term: TermT{
				Type:  TermJqYaml,
				Value: ".field",
			},
			input:    "key: value: invalid",
			expected: false,
		},
		{
			name: "ValidJSONButNoMatch",
			term: TermT{
				Type:  TermJqJson,
				Value: "select(.missing_field == \"value\")",
			},
			input:    "{\"field\": \"value\"}",
			expected: false,
		},
		{
			name: "ValidYAMLButNoMatch",
			term: TermT{
				Type:  TermJqYaml,
				Value: "select(.missing_field == \"value\")",
			},
			input:    "field: value",
			expected: false,
		},
		{
			name: "JSONQueryWithRuntimeError",
			term: TermT{
				Type:  TermJqJson,
				Value: ".field | error(\"test error\")",
			},
			input:    "{\"field\": \"value\"}",
			expected: false, // Runtime error should return false
		},
		{
			name: "JSONQueryWithHaltError",
			term: TermT{
				Type:  TermJqJson,
				Value: ".field | halt",
			},
			input:    "{\"field\": \"value\"}",
			expected: false, // Halt should break the loop
		},
		{
			name: "JSONBooleanTrueResult",
			term: TermT{
				Type:  TermJqJson,
				Value: ".field == \"value\"",
			},
			input:    "{\"field\": \"value\"}",
			expected: true,
		},
		{
			name: "JSONBooleanFalseResult",
			term: TermT{
				Type:  TermJqJson,
				Value: ".field == \"wrong\"",
			},
			input:    "{\"field\": \"value\"}",
			expected: false,
		},
		{
			name: "JSONNonBooleanTruthyResult",
			term: TermT{
				Type:  TermJqJson,
				Value: ".field",
			},
			input:    "{\"field\": \"value\"}",
			expected: true, // Non-boolean non-null result should be truthy
		},
		{
			name: "JSONNullResult",
			term: TermT{
				Type:  TermJqJson,
				Value: ".missing_field",
			},
			input:    "{\"field\": \"value\"}",
			expected: false, // Null result should be falsy
		},
		{
			name: "EmptyStringInput",
			term: TermT{
				Type:  TermJqJson,
				Value: ".field",
			},
			input:    "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher, err := makeJqMatch(tt.term)
			if err != nil {
				t.Fatalf("Failed to create matcher: %v", err)
			}

			result := matcher(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %v for input %q, got %v", tt.expected, tt.input, result)
			}
		})
	}
}

func TestJqMatchCompileErrors(t *testing.T) {
	// Test cases that should fail during jq query compilation
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "InvalidFunction",
			query: ".field | invalid_function",
		},
		{
			name:  "InvalidSyntax",
			query: ".field |",
		},
		{
			name:  "UnclosedBracket",
			query: ".field[",
		},
		{
			name:  "InvalidOperator",
			query: ".field @@@ \"value\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			term := TermT{
				Type:  TermJqJson,
				Value: tt.query,
			}

			_, err := makeJqMatch(term)
			if err == nil {
				t.Errorf("Expected compilation error for query %q, but got nil", tt.query)
			}
		})
	}
}

func TestJqMatchWithCachedUnmarshaling(t *testing.T) {
	term := TermT{
		Type:  TermJqJson,
		Value: ".field",
	}

	matcher, err := makeJqMatch(term)
	if err != nil {
		t.Fatalf("Failed to create matcher: %v", err)
	}

	input := "{\"field\": \"value\"}"

	// First call - should unmarshal
	result1 := matcher(input)
	if !result1 {
		t.Error("Expected true for first call")
	}

	// Second call with same input - should use cached result
	result2 := matcher(input)
	if !result2 {
		t.Error("Expected true for cached call")
	}

	// Third call with different input - should unmarshal again
	result3 := matcher("{\"field\": \"different\"}")
	if !result3 {
		t.Error("Expected true for different input")
	}
}

func TestJqMatchErrorHandling(t *testing.T) {
	// Test that runtime errors in jq queries are handled gracefully
	term := TermT{
		Type:  TermJqJson,
		Value: ".field | if type == \"string\" then . else error(\"not a string\") end",
	}

	matcher, err := makeJqMatch(term)
	if err != nil {
		t.Fatalf("Failed to create matcher: %v", err)
	}

	// This should trigger the error path in the jq query
	result := matcher("{\"field\": 123}")
	if result {
		t.Error("Expected false when jq query has runtime error")
	}
}
