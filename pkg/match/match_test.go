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
