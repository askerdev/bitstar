package filtering

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	kindComparator = "COMPARATOR"
	kindNegate     = "NEGATE"
	kindAnd        = "AND"
	kindOr         = "OR"
	kindDot        = "DOT"
	kindLParen     = "LPAREN"
	kindRParen     = "RPAREN"
	kindComma      = "COMMA"
	kindString     = "STRING"
	kindText       = "TEXT"
	kindEnd        = "END"
)

var lexerRegexp = regexp.MustCompile(`^(<=|>=|!=|<|>|=|\:)|(NOT\s)|(-)(?:\S)|(AND\s)|(OR\s)|(\.)|(\()|(\))|(,)|("(?:[^"\\]|\\.)*")|([^\s\.,<>=!:\(\)]+)`)

type token struct {
	kind  string
	value string
}

type filterLexer struct {
	input string
	next  *token
}

func NewLexer(input string) *filterLexer {
	return &filterLexer{input: input}
}

func (l *filterLexer) Peek() (*token, error) {
	if l.next == nil {
		var err error
		l.next, err = l.Next()
		if err != nil {
			return nil, err
		}
	}
	return l.next, nil
}

func (l *filterLexer) Next() (*token, error) {
	if l.next != nil {
		next := l.next
		l.next = nil
		return next, nil
	}
	l.next = nil
	l.input = strings.TrimLeft(l.input, " \t\r\n")
	if l.input == "" {
		return &token{kind: kindEnd}, nil
	}
	matches := lexerRegexp.FindStringSubmatch(l.input)
	if matches == nil {
		return nil, fmt.Errorf("error: unable to lex token from %q", l.input)
	}
	if matches[3] != "" {
		l.input = l.input[1:]
		return &token{kind: kindNegate, value: "-"}, nil
	}
	l.input = l.input[len(matches[0]):]
	if matches[1] != "" {
		return &token{kind: kindComparator, value: matches[1]}, nil
	}
	if matches[2] != "" {
		length := len(matches[2])
		return &token{kind: kindNegate, value: matches[2][:length-1]}, nil
	}
	if matches[4] != "" {
		length := len(matches[4])
		return &token{kind: kindAnd, value: matches[4][:length-1]}, nil
	}
	if matches[5] != "" {
		length := len(matches[5])
		return &token{kind: kindOr, value: matches[5][:length-1]}, nil
	}
	if matches[6] != "" {
		return &token{kind: kindDot, value: matches[6]}, nil
	}
	if matches[7] != "" {
		return &token{kind: kindLParen, value: matches[7]}, nil
	}
	if matches[8] != "" {
		return &token{kind: kindRParen, value: matches[8]}, nil
	}
	if matches[9] != "" {
		return &token{kind: kindComma, value: matches[9]}, nil
	}
	if matches[10] != "" {
		return &token{kind: kindString, value: matches[10]}, nil
	}
	if matches[11] != "" {
		return &token{kind: kindText, value: matches[11]}, nil
	}
	return nil, fmt.Errorf("error: unhandled lexer regexp match %q", matches[0])
}

type Filter struct {
	Expression *Expression
}

func (v *Filter) String() string {
	var s strings.Builder
	s.WriteString("filter{")
	if v.Expression != nil {
		s.WriteString(v.Expression.String())
	}
	s.WriteString("}")
	return s.String()
}

type Expression struct {
	Sequences []*Sequence
}

func (v *Expression) String() string {
	var s strings.Builder
	s.WriteString("expression{")
	for i, c := range v.Sequences {
		if i > 0 {
			s.WriteString(",")
		}
		if c != nil {
			s.WriteString(c.String())
		}
	}
	s.WriteString("}")
	return s.String()
}

type Sequence struct {
	Factors []*Factor
}

func (v *Sequence) String() string {
	var s strings.Builder
	s.WriteString("sequence{")
	for i, c := range v.Factors {
		if i > 0 {
			s.WriteString(",")
		}
		if c != nil {
			s.WriteString(c.String())
		}
	}
	s.WriteString("}")
	return s.String()
}

type Factor struct {
	Terms []*Term
}

func (v *Factor) String() string {
	var s strings.Builder
	s.WriteString("factor{")
	for i, c := range v.Terms {
		if i > 0 {
			s.WriteString(",")
		}
		if c != nil {
			s.WriteString(c.String())
		}
	}
	s.WriteString("}")
	return s.String()
}

type Term struct {
	Negated bool
	Simple  *Simple
}

func (v *Term) String() string {
	var s strings.Builder
	s.WriteString("term{")
	if v.Negated {
		s.WriteString("-")
	}
	if v.Simple != nil {
		s.WriteString(v.Simple.String())
	}
	s.WriteString("}")
	return s.String()
}

type Simple struct {
	Restriction *Restriction
	Composite   *Expression
}

func (v *Simple) String() string {
	var s strings.Builder
	s.WriteString("simple{")
	if v.Restriction != nil {
		s.WriteString(v.Restriction.String())
	}
	if v.Restriction != nil && v.Composite != nil {
		s.WriteString(",")
	}
	if v.Composite != nil {
		s.WriteString(v.Composite.String())
	}
	s.WriteString("}")
	return s.String()
}

type Restriction struct {
	Comparable *Comparable
	Comparator string
	Arg        *Arg
}

func (v *Restriction) String() string {
	var s strings.Builder
	s.WriteString("restriction{")
	if v.Comparable != nil {
		s.WriteString(v.Comparable.String())
	}
	if v.Comparator != "" {
		s.WriteString(",")
		s.WriteString(strconv.Quote(v.Comparator))
	}
	if v.Arg != nil {
		s.WriteString(",")
		s.WriteString(v.Arg.String())
	}
	s.WriteString("}")
	return s.String()
}

type Arg struct {
	Comparable *Comparable
	Composite  *Expression
}

func (v *Arg) String() string {
	var s strings.Builder
	s.WriteString("arg{")
	if v.Comparable != nil {
		s.WriteString(v.Comparable.String())
	}
	if v.Comparable != nil && v.Composite != nil {
		s.WriteString(",")
	}
	if v.Composite != nil {
		s.WriteString(v.Composite.String())
	}
	s.WriteString("}")
	return s.String()
}

type Comparable struct {
	Member *Member
}

func (v *Comparable) String() string {
	var s strings.Builder
	s.WriteString("comparable{")
	if v.Member != nil {
		s.WriteString(v.Member.String())
	}
	s.WriteString("}")
	return s.String()
}

type Member struct {
	Value  *Value
	Fields []*Value
}

func (v *Member) String() string {
	var s strings.Builder
	s.WriteString("member{")
	s.WriteString(v.Value.String())
	if len(v.Fields) > 0 {
		s.WriteString(", {")
	}
	for i, c := range v.Fields {
		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString(c.String())
	}
	if len(v.Fields) > 0 {
		s.WriteString("}")
	}
	s.WriteString("}")
	return s.String()
}

func (v Member) Input() string {
	var s strings.Builder
	if v.Value != nil {
		s.WriteString(v.Value.Input())
	}
	for _, f := range v.Fields {
		s.WriteString(".")
		s.WriteString(f.Input())
	}
	return s.String()
}

type Value struct {
	Quoted bool
	Value  string
}

func (v Value) String() string {
	var s strings.Builder
	s.WriteString("value{")
	if v.Quoted {
		s.WriteString("quoted,")
	}
	s.WriteString(strconv.Quote(v.Value))
	s.WriteString("}")
	return s.String()
}

func (v Value) Input() string {
	if v.Quoted {
		return strconv.Quote(v.Value)
	}
	return v.Value
}

func ParseFilter(filter string) (*Filter, error) {
	return newParser(filter).filter()
}

type parser struct {
	lexer filterLexer
}

func newParser(input string) *parser {
	return &parser{lexer: *NewLexer(input)}
}

func (p *parser) expect(kind string) error {
	t, err := p.lexer.Peek()
	if err != nil {
		return err
	}
	if t.kind != kind {
		return fmt.Errorf("expected %s but got %s(%q)", kind, t.kind, t.value)
	}
	_, err = p.lexer.Next()
	return err
}

func (p *parser) accept(kind string) (*token, error) {
	t, err := p.lexer.Peek()
	if err != nil {
		return nil, err
	}
	if t.kind != kind {
		return nil, nil
	}
	return p.lexer.Next()
}

func (p *parser) filter() (*Filter, error) {
	t, err := p.accept(kindEnd)
	if err != nil {
		return nil, err
	}
	if t != nil {
		return &Filter{}, nil
	}
	e, err := p.expression()
	if err != nil {
		return nil, err
	}
	return &Filter{Expression: e}, p.expect(kindEnd)
}

func (p *parser) expression() (*Expression, error) {
	s, err := p.sequence()
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}
	e := &Expression{}
	e.Sequences = append(e.Sequences, s)
	for {
		and, err := p.accept(kindAnd)
		if err != nil {
			return nil, err
		}
		if and == nil {
			break
		}
		s, err := p.sequence()
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, fmt.Errorf("expected sequence after AND")
		}
		e.Sequences = append(e.Sequences, s)
	}
	return e, nil
}

func (p *parser) sequence() (*Sequence, error) {
	s := &Sequence{}
	for {
		f, err := p.factor()
		if err != nil {
			return nil, err
		}
		if f == nil {
			break
		}
		s.Factors = append(s.Factors, f)
	}
	if len(s.Factors) == 0 {
		return nil, nil
	}
	return s, nil
}

func (p *parser) factor() (*Factor, error) {
	t, err := p.term()
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, nil
	}
	f := &Factor{}
	f.Terms = append(f.Terms, t)
	for {
		or, err := p.accept(kindOr)
		if err != nil {
			return nil, err
		}
		if or == nil {
			break
		}
		t, err := p.term()
		if err != nil {
			return nil, err
		}
		if t == nil {
			return nil, fmt.Errorf("expected term after OR")
		}
		f.Terms = append(f.Terms, t)
	}
	return f, nil
}

func (p *parser) term() (*Term, error) {
	n, err := p.accept(kindNegate)
	if err != nil {
		return nil, err
	}
	s, err := p.simple()
	if err != nil {
		return nil, err
	}
	if s == nil {
		if n != nil {
			return nil, fmt.Errorf("expected simple term after negation %q", n.value)
		}
		return nil, nil
	}
	return &Term{Negated: n != nil, Simple: s}, nil
}

func (p *parser) simple() (*Simple, error) {
	r, err := p.restriction()
	if err != nil {
		return nil, err
	}
	if r != nil {
		return &Simple{Restriction: r}, nil
	}
	c, err := p.composite()
	if err != nil {
		return nil, err
	}
	if c != nil {
		return &Simple{Composite: c}, nil
	}
	return nil, nil
}

func (p *parser) restriction() (*Restriction, error) {
	comparable, err := p.comparable()
	if err != nil {
		return nil, err
	}
	if comparable == nil {
		return nil, nil
	}
	comparator, err := p.accept(kindComparator)
	if err != nil {
		return nil, err
	}
	if comparator == nil {
		return &Restriction{Comparable: comparable}, nil
	}
	arg, err := p.arg()
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, fmt.Errorf("expected arg after %s", comparator.value)
	}
	return &Restriction{Comparable: comparable, Comparator: comparator.value, Arg: arg}, nil
}

func (p *parser) comparable() (*Comparable, error) {
	m, err := p.member()
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, nil
	}
	return &Comparable{Member: m}, nil
}

func (p *parser) member() (*Member, error) {
	v, err := p.value()
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}

	m := &Member{Value: v}
	for {
		dot, err := p.accept(kindDot)
		if err != nil {
			return nil, err
		}
		if dot == nil {
			break
		}

		v, err := p.value()
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, fmt.Errorf("expected value after '.'")
		}

		m.Fields = append(m.Fields, v)
	}
	return m, nil
}

func (p *parser) value() (*Value, error) {
	v, err := p.accept(kindString)
	if err != nil {
		return nil, err
	}
	if v != nil {
		v.value, err = strconv.Unquote(v.value)
		if err != nil {
			return nil, fmt.Errorf("error unquoting string: %w", err)
		}
		return &Value{Quoted: true, Value: v.value}, nil
	}

	v, err = p.accept(kindText)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	return &Value{Value: v.value}, nil
}

func (p *parser) composite() (*Expression, error) {
	lparen, err := p.accept(kindLParen)
	if err != nil {
		return nil, err
	}
	if lparen == nil {
		return nil, nil
	}
	e, err := p.expression()
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, fmt.Errorf("expected expression")
	}
	return e, p.expect(kindRParen)
}

func (p *parser) arg() (*Arg, error) {
	comparable, err := p.comparable()
	if err != nil {
		return nil, err
	}
	if comparable != nil {
		return &Arg{Comparable: comparable}, nil
	}
	composite, err := p.composite()
	if err != nil {
		return nil, err
	}
	if composite != nil {
		return &Arg{Composite: composite}, nil
	}
	return nil, nil
}
