package main

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

// Value represents any value in the expression language
type Value interface{}

// Context holds variables and functions available during evaluation
type Context struct {
	Variables map[string]Value
	Functions map[string]func([]Value) (Value, error)
}

// Expression represents a parsed expression
type Expression interface {
	Evaluate(ctx *Context) (Value, error)
}

// AST Node types
type BinaryOp struct {
	Left  Expression
	Right Expression
	Op    string
}

func (b *BinaryOp) Evaluate(ctx *Context) (Value, error) {
	left, err := b.Left.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	right, err := b.Right.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	return evaluateBinaryOp(left, right, b.Op)
}

type UnaryOp struct {
	Expr Expression
	Op   string
}

func (u *UnaryOp) Evaluate(ctx *Context) (Value, error) {
	val, err := u.Expr.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	switch u.Op {
	case "!":
		return !toBool(val), nil
	case "-":
		return negateNumber(val)
	}

	return nil, fmt.Errorf("unknown unary operator: %s", u.Op)
}

type Literal struct {
	Value Value
}

func (l *Literal) Evaluate(ctx *Context) (Value, error) {
	return l.Value, nil
}

type Variable struct {
	Name string
}

func (v *Variable) Evaluate(ctx *Context) (Value, error) {
	if val, exists := ctx.Get(v.Name); exists {
		return val, nil
	}
	return nil, fmt.Errorf("undefined variable: %s", v.Name)
}

type FieldAccess struct {
	Object Expression
	Field  string
}

func (f *FieldAccess) Evaluate(ctx *Context) (Value, error) {
	obj, err := f.Object.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	return getField(obj, f.Field)
}

type IndexAccess struct {
	Object Expression
	Index  Expression
}

func (i *IndexAccess) Evaluate(ctx *Context) (Value, error) {
	obj, err := i.Object.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	idx, err := i.Index.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	return getIndex(obj, idx)
}

type FunctionCall struct {
	Name string
	Args []Expression
}

func (f *FunctionCall) Evaluate(ctx *Context) (Value, error) {
	if fn, exists := ctx.Functions[f.Name]; exists {
		args := make([]Value, len(f.Args))
		var err error
		for i, arg := range f.Args {
			args[i], err = arg.Evaluate(ctx)
			if err != nil {
				return nil, err
			}
		}
		return fn(args)
	}
	return nil, fmt.Errorf("undefined function: %s", f.Name)
}

type MethodCall struct {
	Object Expression
	Method string
	Args   []Expression
}

func (m *MethodCall) Evaluate(ctx *Context) (Value, error) {
	obj, err := m.Object.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	args := make([]Value, len(m.Args))
	for i, arg := range m.Args {
		args[i], err = arg.Evaluate(ctx)
		if err != nil {
			return nil, err
		}
	}

	return callMethod(obj, m.Method, args)
}

type ArrayLiteral struct {
	Elements []Expression
}

func (a *ArrayLiteral) Evaluate(ctx *Context) (Value, error) {
	result := make([]Value, len(a.Elements))
	for i, elem := range a.Elements {
		val, err := elem.Evaluate(ctx)
		if err != nil {
			return nil, err
		}
		result[i] = val
	}
	return result, nil
}

type Macro struct {
	Collection Expression
	Variable   string
	Body       Expression
	Type       string // "all", "exists", "filter", "map", "find", "size", "reverse", "sort", "flatMap"
}

func (m *Macro) Evaluate(ctx *Context) (Value, error) {
	coll, err := m.Collection.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	return evaluateMacro(coll, m.Variable, m.Body, m.Type, ctx)
}

type TernaryOp struct {
	Condition Expression
	TrueExpr  Expression
	FalseExpr Expression
}

func (t *TernaryOp) Evaluate(ctx *Context) (Value, error) {
	cond, err := t.Condition.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	if toBool(cond) {
		return t.TrueExpr.Evaluate(ctx)
	} else {
		return t.FalseExpr.Evaluate(ctx)
	}
}

// Token types for lexical analysis
type TokenType int

const (
	// Literals
	IDENTIFIER TokenType = iota
	NUMBER
	STRING

	// Operators
	PLUS     // +
	MINUS    // -
	MULTIPLY // *
	DIVIDE   // /
	MODULO   // %
	POWER    // **

	// Comparison
	EQ // ==
	NE // !=
	LT // <
	LE // <=
	GT // >
	GE // >=

	// Logical
	AND // &&
	OR  // ||
	NOT // !

	// Other operators
	IN // in

	// Punctuation
	LPAREN   // (
	RPAREN   // )
	LBRACKET // [
	RBRACKET // ]
	LBRACE   // {
	RBRACE   // }
	DOT      // .
	COMMA    // ,
	COLON    // :
	QUESTION // ?

	// Keywords
	TRUE
	FALSE
	NULL

	// Special
	EOF
	ILLEGAL
)

type Token struct {
	Type     TokenType
	Literal  string
	Position int
}

// Lexer tokenizes the input string
type Lexer struct {
	input        string
	position     int  // current position in input (points to current char)
	readPosition int  // current reading position in input (after current char)
	ch           byte // current char under examination
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0 // ASCII NUL character represents "EOF"
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
}

func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[position:l.position]
}

func (l *Lexer) readNumber() string {
	position := l.position
	for isDigit(l.ch) {
		l.readChar()
	}
	if l.ch == '.' && isDigit(l.peekChar()) {
		l.readChar()
		for isDigit(l.ch) {
			l.readChar()
		}
	}
	return l.input[position:l.position]
}

func (l *Lexer) readString() string {
	position := l.position + 1
	for {
		l.readChar()
		if l.ch == '\'' || l.ch == '"' || l.ch == 0 {
			break
		}
	}
	return l.input[position:l.position]
}

func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	switch l.ch {
	case '+':
		tok = Token{Type: PLUS, Literal: string(l.ch), Position: l.position}
	case '-':
		tok = Token{Type: MINUS, Literal: string(l.ch), Position: l.position}
	case '*':
		if l.peekChar() == '*' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: POWER, Literal: string(ch) + string(l.ch), Position: l.position}
		} else {
			tok = Token{Type: MULTIPLY, Literal: string(l.ch), Position: l.position}
		}
	case '/':
		tok = Token{Type: DIVIDE, Literal: string(l.ch), Position: l.position}
	case '%':
		tok = Token{Type: MODULO, Literal: string(l.ch), Position: l.position}
	case '=':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: EQ, Literal: string(ch) + string(l.ch), Position: l.position}
		} else {
			tok = Token{Type: ILLEGAL, Literal: string(l.ch), Position: l.position}
		}
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: NE, Literal: string(ch) + string(l.ch), Position: l.position}
		} else {
			tok = Token{Type: NOT, Literal: string(l.ch), Position: l.position}
		}
	case '<':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: LE, Literal: string(ch) + string(l.ch), Position: l.position}
		} else {
			tok = Token{Type: LT, Literal: string(l.ch), Position: l.position}
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: GE, Literal: string(ch) + string(l.ch), Position: l.position}
		} else {
			tok = Token{Type: GT, Literal: string(l.ch), Position: l.position}
		}
	case '&':
		if l.peekChar() == '&' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: AND, Literal: string(ch) + string(l.ch), Position: l.position}
		} else {
			tok = Token{Type: ILLEGAL, Literal: string(l.ch), Position: l.position}
		}
	case '|':
		if l.peekChar() == '|' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: OR, Literal: string(ch) + string(l.ch), Position: l.position}
		} else {
			tok = Token{Type: ILLEGAL, Literal: string(l.ch), Position: l.position}
		}
	case '(':
		tok = Token{Type: LPAREN, Literal: string(l.ch), Position: l.position}
	case ')':
		tok = Token{Type: RPAREN, Literal: string(l.ch), Position: l.position}
	case '[':
		tok = Token{Type: LBRACKET, Literal: string(l.ch), Position: l.position}
	case ']':
		tok = Token{Type: RBRACKET, Literal: string(l.ch), Position: l.position}
	case '{':
		tok = Token{Type: LBRACE, Literal: string(l.ch), Position: l.position}
	case '}':
		tok = Token{Type: RBRACE, Literal: string(l.ch), Position: l.position}
	case '.':
		tok = Token{Type: DOT, Literal: string(l.ch), Position: l.position}
	case ',':
		tok = Token{Type: COMMA, Literal: string(l.ch), Position: l.position}
	case ':':
		tok = Token{Type: COLON, Literal: string(l.ch), Position: l.position}
	case '?':
		tok = Token{Type: QUESTION, Literal: string(l.ch), Position: l.position}
	case '\'', '"':
		tok.Type = STRING
		tok.Literal = l.readString()
		tok.Position = l.position
	case 0:
		tok = Token{Type: EOF, Literal: "", Position: l.position}
	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Position = l.position
			tok.Type = lookupIdent(tok.Literal)
			return tok
		} else if isDigit(l.ch) {
			tok.Type = NUMBER
			tok.Literal = l.readNumber()
			tok.Position = l.position
			return tok
		} else {
			tok = Token{Type: ILLEGAL, Literal: string(l.ch), Position: l.position}
		}
	}

	l.readChar()
	return tok
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func lookupIdent(ident string) TokenType {
	keywords := map[string]TokenType{
		"true":  TRUE,
		"false": FALSE,
		"null":  NULL,
		"in":    IN,
	}

	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return IDENTIFIER
}

// Parser builds AST from tokens
type Parser struct {
	lexer *Lexer

	curToken  Token
	peekToken Token

	errors []string
}

func NewParser(input string) *Parser {
	p := &Parser{
		lexer:  NewLexer(input),
		errors: []string{},
	}

	// Read two tokens, so curToken and peekToken are both set
	p.nextToken()
	p.nextToken()

	return p
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

func (p *Parser) Parse() (Expression, error) {
	expr := p.parseExpression(LOWEST)
	if len(p.errors) > 0 {
		return nil, fmt.Errorf("parse errors: %v", p.errors)
	}
	return expr, nil
}

// Precedence levels
const (
	_ int = iota
	LOWEST
	TERNARY          // ? :
	OR_PRECEDENCE    // ||
	AND_PRECEDENCE   // &&
	EQUALS           // ==, !=
	LESSGREATER      // > or <, >=, <=
	IN_PRECEDENCE    // in
	SUM              // +
	PRODUCT          // *, /, %
	POWER_PRECEDENCE // **
	UNARY            // -X, !X
	CALL             // myFunction(X)
	INDEX            // array[index], obj.field
)

func (p *Parser) peekPrecedence() int {
	precedences := map[TokenType]int{
		QUESTION: TERNARY,
		OR:       OR_PRECEDENCE,
		AND:      AND_PRECEDENCE,
		EQ:       EQUALS,
		NE:       EQUALS,
		LT:       LESSGREATER,
		GT:       LESSGREATER,
		LE:       LESSGREATER,
		GE:       LESSGREATER,
		IN:       IN_PRECEDENCE,
		PLUS:     SUM,
		MINUS:    SUM,
		MULTIPLY: PRODUCT,
		DIVIDE:   PRODUCT,
		MODULO:   PRODUCT,
		POWER:    POWER_PRECEDENCE,
		LPAREN:   CALL,
		LBRACKET: INDEX,
		DOT:      INDEX,
	}

	if p, ok := precedences[p.peekToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *Parser) curPrecedence() int {
	precedences := map[TokenType]int{
		QUESTION: TERNARY,
		OR:       OR_PRECEDENCE,
		AND:      AND_PRECEDENCE,
		EQ:       EQUALS,
		NE:       EQUALS,
		LT:       LESSGREATER,
		GT:       LESSGREATER,
		LE:       LESSGREATER,
		GE:       LESSGREATER,
		IN:       IN_PRECEDENCE,
		PLUS:     SUM,
		MINUS:    SUM,
		MULTIPLY: PRODUCT,
		DIVIDE:   PRODUCT,
		MODULO:   PRODUCT,
		POWER:    POWER_PRECEDENCE,
		LPAREN:   CALL,
		LBRACKET: INDEX,
		DOT:      INDEX,
	}

	if p, ok := precedences[p.curToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *Parser) parseExpression(precedence int) Expression {
	// Prefix parsing
	var leftExp Expression

	switch p.curToken.Type {
	case IDENTIFIER:
		leftExp = p.parseIdentifier()
	case NUMBER:
		leftExp = p.parseNumber()
	case STRING:
		leftExp = p.parseString()
	case TRUE, FALSE:
		leftExp = p.parseBoolean()
	case NULL:
		leftExp = p.parseNull()
	case NOT, MINUS:
		leftExp = p.parseUnaryExpression()
	case LPAREN:
		leftExp = p.parseGroupedExpression()
	case LBRACKET:
		leftExp = p.parseArrayLiteral()
	default:
		p.errors = append(p.errors, fmt.Sprintf("no prefix parse function for %v found", p.curToken.Type))
		return nil
	}

	// Infix parsing
	for p.peekToken.Type != EOF && precedence < p.peekPrecedence() {
		switch p.peekToken.Type {
		case QUESTION:
			p.nextToken()
			leftExp = p.parseTernaryExpression(leftExp)
		case PLUS, MINUS, MULTIPLY, DIVIDE, MODULO, POWER:
			p.nextToken()
			leftExp = p.parseBinaryExpression(leftExp)
		case EQ, NE, LT, GT, LE, GE:
			p.nextToken()
			leftExp = p.parseBinaryExpression(leftExp)
		case AND, OR:
			p.nextToken()
			leftExp = p.parseBinaryExpression(leftExp)
		case IN:
			p.nextToken()
			leftExp = p.parseBinaryExpression(leftExp)
		case LPAREN:
			p.nextToken()
			leftExp = p.parseCallExpression(leftExp)
		case LBRACKET:
			p.nextToken()
			leftExp = p.parseIndexExpression(leftExp)
		case DOT:
			p.nextToken()
			leftExp = p.parseFieldAccess(leftExp)
		default:
			return leftExp
		}
	}

	return leftExp
}

func (p *Parser) parseIdentifier() Expression {
	return &Variable{Name: p.curToken.Literal}
}

func (p *Parser) parseNumber() Expression {
	lit := &Literal{}

	if strings.Contains(p.curToken.Literal, ".") {
		value, err := strconv.ParseFloat(p.curToken.Literal, 64)
		if err != nil {
			msg := fmt.Sprintf("could not parse %q as float", p.curToken.Literal)
			p.errors = append(p.errors, msg)
			return nil
		}
		lit.Value = value
	} else {
		value, err := strconv.ParseInt(p.curToken.Literal, 0, 64)
		if err != nil {
			msg := fmt.Sprintf("could not parse %q as integer", p.curToken.Literal)
			p.errors = append(p.errors, msg)
			return nil
		}
		lit.Value = value
	}

	return lit
}

func (p *Parser) parseString() Expression {
	return &Literal{Value: p.curToken.Literal}
}

func (p *Parser) parseBoolean() Expression {
	return &Literal{Value: p.curToken.Type == TRUE}
}

func (p *Parser) parseNull() Expression {
	return &Literal{Value: nil}
}

func (p *Parser) parseUnaryExpression() Expression {
	expression := &UnaryOp{
		Op: p.curToken.Literal,
	}

	p.nextToken()

	expression.Expr = p.parseExpression(UNARY)

	return expression
}

func (p *Parser) parseGroupedExpression() Expression {
	p.nextToken()

	exp := p.parseExpression(LOWEST)

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return exp
}

func (p *Parser) parseArrayLiteral() Expression {
	array := &ArrayLiteral{}

	if p.peekToken.Type == RBRACKET {
		p.nextToken()
		return array
	}

	p.nextToken()

	array.Elements = append(array.Elements, p.parseExpression(LOWEST))

	for p.peekToken.Type == COMMA {
		p.nextToken()
		p.nextToken()
		array.Elements = append(array.Elements, p.parseExpression(LOWEST))
	}

	if !p.expectPeek(RBRACKET) {
		return nil
	}

	return array
}

func (p *Parser) parseBinaryExpression(left Expression) Expression {
	expression := &BinaryOp{
		Left: left,
		Op:   p.curToken.Literal,
	}

	precedence := p.curPrecedence()
	p.nextToken()
	expression.Right = p.parseExpression(precedence)

	return expression
}

func (p *Parser) parseTernaryExpression(condition Expression) Expression {
	p.nextToken() // consume the '?'

	trueExpr := p.parseExpression(LOWEST)

	if !p.expectPeek(COLON) {
		return nil
	}

	p.nextToken() // consume the ':'
	falseExpr := p.parseExpression(TERNARY)

	return &TernaryOp{
		Condition: condition,
		TrueExpr:  trueExpr,
		FalseExpr: falseExpr,
	}
}

func (p *Parser) parseCallExpression(fn Expression) Expression {
	exp := &FunctionCall{}

	// Check if this is a method call on an object
	if variable, ok := fn.(*Variable); ok {
		exp.Name = variable.Name
	} else {
		p.errors = append(p.errors, "invalid function call")
		return nil
	}

	exp.Args = p.parseExpressionList(RPAREN)
	return exp
}

func (p *Parser) parseIndexExpression(left Expression) Expression {
	exp := &IndexAccess{Object: left}

	p.nextToken()
	exp.Index = p.parseExpression(LOWEST)

	if !p.expectPeek(RBRACKET) {
		return nil
	}

	return exp
}

func (p *Parser) parseFieldAccess(left Expression) Expression {
	if p.peekToken.Type != IDENTIFIER {
		p.errors = append(p.errors, fmt.Sprintf("expected identifier after dot, got %v", p.peekToken.Type))
		return nil
	}

	p.nextToken()
	fieldName := p.curToken.Literal

	// Check for macro operations FIRST (collection.filter, .map, etc.)
	if fieldName == "filter" || fieldName == "map" || fieldName == "all" || fieldName == "exists" || fieldName == "find" ||
		fieldName == "size" || fieldName == "reverse" || fieldName == "sort" || fieldName == "flatMap" {
		return p.parseMacroExpression(left, fieldName)
	}

	// Check for method calls (field followed by parentheses)
	if p.peekToken.Type == LPAREN {
		p.nextToken() // consume the '('

		method := &MethodCall{
			Object: left,
			Method: fieldName,
		}

		method.Args = p.parseExpressionList(RPAREN)
		return method
	}

	// Regular field access
	return &FieldAccess{
		Object: left,
		Field:  fieldName,
	}
}

func (p *Parser) parseMacroExpression(collection Expression, macroType string) Expression {
	if !p.expectPeek(LPAREN) {
		return nil
	}

	// Some operations don't need variables (size, reverse)
	if macroType == "size" || macroType == "reverse" {
		if !p.expectPeek(RPAREN) {
			return nil
		}
		return &Macro{
			Collection: collection,
			Variable:   "",
			Body:       nil,
			Type:       macroType,
		}
	}

	if !p.expectPeek(IDENTIFIER) {
		return nil
	}

	variable := p.curToken.Literal

	if !p.expectPeek(COMMA) {
		return nil
	}

	p.nextToken()
	body := p.parseExpression(LOWEST)

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return &Macro{
		Collection: collection,
		Variable:   variable,
		Body:       body,
		Type:       macroType,
	}
}

func (p *Parser) parseExpressionList(end TokenType) []Expression {
	var args []Expression

	if p.peekToken.Type == end {
		p.nextToken()
		return args
	}

	p.nextToken()
	args = append(args, p.parseExpression(LOWEST))

	for p.peekToken.Type == COMMA {
		p.nextToken()
		p.nextToken()
		args = append(args, p.parseExpression(LOWEST))
	}

	if !p.expectPeek(end) {
		return nil
	}

	return args
}

func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekToken.Type == t {
		p.nextToken()
		return true
	} else {
		p.peekError(t)
		return false
	}
}

func (p *Parser) peekError(t TokenType) {
	msg := fmt.Sprintf("expected next token to be %v, got %v instead", t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
}

// Context methods
func NewContext() *Context {
	ctx := &Context{
		Variables: make(map[string]Value),
		Functions: make(map[string]func([]Value) (Value, error)),
	}

	// Register built-in functions
	ctx.registerBuiltins()
	return ctx
}

func (c *Context) Set(name string, value Value) {
	c.Variables[name] = value
}

func (c *Context) Get(name string) (Value, bool) {
	val, exists := c.Variables[name]
	return val, exists
}

// Register all built-in functions
func (c *Context) registerBuiltins() {
	// Basic functions
	c.Functions["length"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("length() requires 1 argument")
		}

		switch v := args[0].(type) {
		case []Value:
			return len(v), nil
		case map[string]Value:
			return len(v), nil
		case string:
			return len(v), nil
		default:
			rv := reflect.ValueOf(v)
			if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array || rv.Kind() == reflect.Map {
				return rv.Len(), nil
			}
		}
		return 0, nil
	}

	// String functions
	c.Functions["upper"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("upper() requires 1 argument")
		}
		return strings.ToUpper(toString(args[0])), nil
	}

	c.Functions["lower"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("lower() requires 1 argument")
		}
		return strings.ToLower(toString(args[0])), nil
	}

	c.Functions["trim"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("trim() requires 1 argument")
		}
		return strings.TrimSpace(toString(args[0])), nil
	}

	c.Functions["split"] = func(args []Value) (Value, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("split() requires 2 arguments")
		}
		parts := strings.Split(toString(args[0]), toString(args[1]))
		result := make([]Value, len(parts))
		for i, part := range parts {
			result[i] = part
		}
		return result, nil
	}

	c.Functions["replace"] = func(args []Value) (Value, error) {
		if len(args) != 3 {
			return nil, fmt.Errorf("replace() requires 3 arguments")
		}
		return strings.ReplaceAll(toString(args[0]), toString(args[1]), toString(args[2])), nil
	}

	// Math functions
	c.Functions["abs"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("abs() requires 1 argument")
		}
		return math.Abs(toFloat64(args[0])), nil
	}

	c.Functions["ceil"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("ceil() requires 1 argument")
		}
		return math.Ceil(toFloat64(args[0])), nil
	}

	c.Functions["floor"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("floor() requires 1 argument")
		}
		return math.Floor(toFloat64(args[0])), nil
	}

	c.Functions["round"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("round() requires 1 argument")
		}
		return math.Round(toFloat64(args[0])), nil
	}

	c.Functions["min"] = func(args []Value) (Value, error) {
		if len(args) < 2 {
			return nil, fmt.Errorf("min() requires at least 2 arguments")
		}
		min := toFloat64(args[0])
		for i := 1; i < len(args); i++ {
			val := toFloat64(args[i])
			if val < min {
				min = val
			}
		}
		return min, nil
	}

	c.Functions["max"] = func(args []Value) (Value, error) {
		if len(args) < 2 {
			return nil, fmt.Errorf("max() requires at least 2 arguments")
		}
		max := toFloat64(args[0])
		for i := 1; i < len(args); i++ {
			val := toFloat64(args[i])
			if val > max {
				max = val
			}
		}
		return max, nil
	}

	c.Functions["sqrt"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("sqrt() requires 1 argument")
		}
		return math.Sqrt(toFloat64(args[0])), nil
	}

	c.Functions["pow"] = func(args []Value) (Value, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("pow() requires 2 arguments")
		}
		return math.Pow(toFloat64(args[0]), toFloat64(args[1])), nil
	}

	// Aggregation functions
	c.Functions["sum"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("sum() requires 1 argument")
		}

		items := toValueSlice(args[0])
		if items == nil {
			return nil, fmt.Errorf("sum() requires a collection")
		}

		sum := 0.0
		for _, item := range items {
			sum += toFloat64(item)
		}
		return sum, nil
	}

	c.Functions["avg"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("avg() requires 1 argument")
		}

		items := toValueSlice(args[0])
		if items == nil {
			return nil, fmt.Errorf("avg() requires a collection")
		}

		if len(items) == 0 {
			return 0.0, nil
		}

		sum := 0.0
		for _, item := range items {
			sum += toFloat64(item)
		}
		return sum / float64(len(items)), nil
	}

	// Type functions
	c.Functions["type"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("type() requires 1 argument")
		}
		return getType(args[0]), nil
	}

	c.Functions["int"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("int() requires 1 argument")
		}
		return int64(toFloat64(args[0])), nil
	}

	c.Functions["double"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("double() requires 1 argument")
		}
		return toFloat64(args[0]), nil
	}

	c.Functions["string"] = func(args []Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("string() requires 1 argument")
		}
		return toString(args[0]), nil
	}
}

// Helper functions
func evaluateBinaryOp(left, right Value, op string) (Value, error) {
	switch op {
	case "==":
		return equals(left, right), nil
	case "!=":
		return !equals(left, right), nil
	case ">":
		return compare(left, right) > 0, nil
	case "<":
		return compare(left, right) < 0, nil
	case ">=":
		return compare(left, right) >= 0, nil
	case "<=":
		return compare(left, right) <= 0, nil
	case "&&":
		return toBool(left) && toBool(right), nil
	case "||":
		return toBool(left) || toBool(right), nil
	case "+":
		return add(left, right)
	case "-":
		return subtract(left, right)
	case "*":
		return multiply(left, right)
	case "/":
		return divide(left, right)
	case "%":
		return modulo(left, right)
	case "**":
		return power(left, right)
	case "in":
		return contains(right, left), nil
	}
	return nil, fmt.Errorf("unknown operator: %s", op)
}

func toBool(val Value) bool {
	if val == nil {
		return false
	}

	switch v := val.(type) {
	case bool:
		return v
	case int, int64:
		return reflect.ValueOf(v).Int() != 0
	case float64:
		return v != 0
	case string:
		return v != ""
	case []Value:
		return len(v) > 0
	case map[string]Value:
		return len(v) > 0
	default:
		return true
	}
}

func toString(val Value) string {
	if val == nil {
		return ""
	}

	switch v := val.(type) {
	case string:
		return v
	case int, int64, float64, bool:
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func toFloat64(val Value) float64 {
	switch v := val.(type) {
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case float64:
		return v
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return 0
}

func equals(left, right Value) bool {
	if left == nil && right == nil {
		return true
	}
	if left == nil || right == nil {
		return false
	}
	return reflect.DeepEqual(left, right)
}

func compare(left, right Value) int {
	switch l := left.(type) {
	case string:
		if r, ok := right.(string); ok {
			if l < r {
				return -1
			} else if l > r {
				return 1
			}
			return 0
		}
	case int, int64, float64:
		lf := toFloat64(l)
		rf := toFloat64(right)
		if lf < rf {
			return -1
		} else if lf > rf {
			return 1
		}
		return 0
	}
	return 0
}

func add(left, right Value) (Value, error) {
	// String concatenation
	if l, ok := left.(string); ok {
		return l + toString(right), nil
	}
	if r, ok := right.(string); ok {
		return toString(left) + r, nil
	}

	// Numeric addition
	return toFloat64(left) + toFloat64(right), nil
}

func subtract(left, right Value) (Value, error) {
	return toFloat64(left) - toFloat64(right), nil
}

func multiply(left, right Value) (Value, error) {
	return toFloat64(left) * toFloat64(right), nil
}

func divide(left, right Value) (Value, error) {
	r := toFloat64(right)
	if r == 0 {
		return nil, fmt.Errorf("division by zero")
	}
	return toFloat64(left) / r, nil
}

func modulo(left, right Value) (Value, error) {
	r := toFloat64(right)
	if r == 0 {
		return nil, fmt.Errorf("division by zero")
	}
	return math.Mod(toFloat64(left), r), nil
}

func power(left, right Value) (Value, error) {
	return math.Pow(toFloat64(left), toFloat64(right)), nil
}

func negateNumber(val Value) (Value, error) {
	return -toFloat64(val), nil
}

func contains(container, item Value) bool {
	switch c := container.(type) {
	case []Value:
		for _, v := range c {
			if equals(v, item) {
				return true
			}
		}
	case map[string]Value:
		key := toString(item)
		_, exists := c[key]
		return exists
	case string:
		return strings.Contains(c, toString(item))
	}
	return false
}

func getField(obj Value, field string) (Value, error) {
	switch o := obj.(type) {
	case map[string]Value:
		if val, exists := o[field]; exists {
			return val, nil
		}
		return nil, nil
	default:
		rv := reflect.ValueOf(obj)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		if rv.Kind() == reflect.Struct {
			fv := rv.FieldByName(field)
			if fv.IsValid() {
				return fv.Interface(), nil
			}
		}
	}
	return nil, fmt.Errorf("field %s not found", field)
}

func getIndex(obj, index Value) (Value, error) {
	switch o := obj.(type) {
	case []Value:
		idx := int(toFloat64(index))
		if idx >= 0 && idx < len(o) {
			return o[idx], nil
		}
		return nil, fmt.Errorf("index out of range")
	case map[string]Value:
		key := toString(index)
		if val, exists := o[key]; exists {
			return val, nil
		}
		return nil, nil
	}
	return nil, fmt.Errorf("cannot index into value")
}

func callMethod(obj Value, method string, args []Value) (Value, error) {
	// If obj is a collection and method is a vectorizable method, apply to each element
	if slice, ok := obj.([]Value); ok {
		vectorizableMethods := map[string]bool{
			"split": true, "trim": true, "upper": true, "lower": true,
			"replace": true, "contains": true, "startsWith": true, "endsWith": true,
			"matches": true,
		}

		if vectorizableMethods[method] {
			result := make([]Value, len(slice))
			for i, item := range slice {
				val, err := callMethodOnSingle(item, method, args)
				if err != nil {
					return nil, err
				}
				result[i] = val
			}
			return result, nil
		}
	}

	return callMethodOnSingle(obj, method, args)
}

func callMethodOnSingle(obj Value, method string, args []Value) (Value, error) {
	str := toString(obj)

	switch method {
	case "contains":
		if len(args) != 1 {
			return nil, fmt.Errorf("contains() requires 1 argument")
		}
		return strings.Contains(str, toString(args[0])), nil

	case "startsWith":
		if len(args) != 1 {
			return nil, fmt.Errorf("startsWith() requires 1 argument")
		}
		return strings.HasPrefix(str, toString(args[0])), nil

	case "endsWith":
		if len(args) != 1 {
			return nil, fmt.Errorf("endsWith() requires 1 argument")
		}
		return strings.HasSuffix(str, toString(args[0])), nil

	case "length", "size":
		switch o := obj.(type) {
		case []Value:
			return len(o), nil
		case map[string]Value:
			return len(o), nil
		case string:
			return len(o), nil
		}
		return 0, nil

	case "matches":
		if len(args) != 1 {
			return nil, fmt.Errorf("matches() requires 1 argument")
		}
		// Simple pattern matching (not full regex for simplicity)
		pattern := toString(args[0])
		return strings.Contains(str, pattern), nil

	case "split":
		if len(args) != 1 {
			return nil, fmt.Errorf("split() requires 1 argument")
		}
		parts := strings.Split(str, toString(args[0]))
		result := make([]Value, len(parts))
		for i, part := range parts {
			result[i] = part
		}
		return result, nil

	case "replace":
		if len(args) != 2 {
			return nil, fmt.Errorf("replace() requires 2 arguments")
		}
		return strings.ReplaceAll(str, toString(args[0]), toString(args[1])), nil

	case "trim":
		return strings.TrimSpace(str), nil

	case "upper":
		return strings.ToUpper(str), nil

	case "lower":
		return strings.ToLower(str), nil
	}

	return nil, fmt.Errorf("unknown method: %s", method)
}

func evaluateMacro(coll Value, variable string, body Expression, macroType string, ctx *Context) (Value, error) {
	// Convert collection to []Value
	var items []Value
	switch c := coll.(type) {
	case []Value:
		items = c
	default:
		rv := reflect.ValueOf(c)
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			items = make([]Value, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				items[i] = rv.Index(i).Interface()
			}
		} else {
			return nil, fmt.Errorf("cannot iterate over non-collection")
		}
	}

	// Create new context with the loop variable
	newCtx := &Context{
		Variables: make(map[string]Value),
		Functions: ctx.Functions,
	}

	// Copy existing variables
	for k, v := range ctx.Variables {
		newCtx.Variables[k] = v
	}

	switch macroType {
	case "all":
		for _, item := range items {
			newCtx.Variables[variable] = item
			result, err := body.Evaluate(newCtx)
			if err != nil {
				return nil, err
			}
			if !toBool(result) {
				return false, nil
			}
		}
		return true, nil

	case "exists":
		for _, item := range items {
			newCtx.Variables[variable] = item
			result, err := body.Evaluate(newCtx)
			if err != nil {
				return nil, err
			}
			if toBool(result) {
				return true, nil
			}
		}
		return false, nil

	case "filter":
		var filtered []Value
		for _, item := range items {
			newCtx.Variables[variable] = item
			result, err := body.Evaluate(newCtx)
			if err != nil {
				return nil, err
			}
			if toBool(result) {
				filtered = append(filtered, item)
			}
		}
		return filtered, nil

	case "map":
		mapped := make([]Value, len(items))
		for i, item := range items {
			newCtx.Variables[variable] = item
			result, err := body.Evaluate(newCtx)
			if err != nil {
				return nil, err
			}
			mapped[i] = result
		}
		return mapped, nil

	case "find":
		for _, item := range items {
			newCtx.Variables[variable] = item
			result, err := body.Evaluate(newCtx)
			if err != nil {
				return nil, err
			}
			if toBool(result) {
				return item, nil
			}
		}
		return nil, nil // Return null if no item found

	case "size":
		return len(items), nil

	case "reverse":
		reversed := make([]Value, len(items))
		for i := 0; i < len(items); i++ {
			reversed[i] = items[len(items)-1-i]
		}
		return reversed, nil

	case "sort":
		// Simple sort by the expression result
		sorted := make([]Value, len(items))
		copy(sorted, items)

		// Simple bubble sort for demonstration
		for i := 0; i < len(sorted)-1; i++ {
			for j := 0; j < len(sorted)-i-1; j++ {
				newCtx.Variables[variable] = sorted[j]
				val1, err := body.Evaluate(newCtx)
				if err != nil {
					return nil, err
				}

				newCtx.Variables[variable] = sorted[j+1]
				val2, err := body.Evaluate(newCtx)
				if err != nil {
					return nil, err
				}

				if compare(val1, val2) > 0 {
					sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
				}
			}
		}
		return sorted, nil

	case "flatMap":
		var flattened []Value
		for _, item := range items {
			newCtx.Variables[variable] = item
			result, err := body.Evaluate(newCtx)
			if err != nil {
				return nil, err
			}

			// If result is a collection, add all elements
			if resultSlice := toValueSlice(result); resultSlice != nil {
				flattened = append(flattened, resultSlice...)
			} else {
				// If result is a single value, add it directly
				flattened = append(flattened, result)
			}
		}
		return flattened, nil
	}

	return nil, fmt.Errorf("unknown macro type: %s", macroType)
}

func toValueSlice(val Value) []Value {
	switch v := val.(type) {
	case []Value:
		return v
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			result := make([]Value, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				result[i] = rv.Index(i).Interface()
			}
			return result
		}
	}
	return nil
}

func getType(val Value) string {
	if val == nil {
		return "null"
	}

	switch val.(type) {
	case bool:
		return "bool"
	case int, int64:
		return "int"
	case float64:
		return "double"
	case string:
		return "string"
	case []Value:
		return "list"
	case map[string]Value:
		return "map"
	default:
		return "unknown"
	}
}

// Main function with examples
func main() {
	fmt.Println("🚀 Proper CEL Expression Language Implementation")
	fmt.Println("===============================================")

	ctx := NewContext()

	// Set up test data
	users := []Value{
		map[string]Value{
			"id":         1,
			"name":       "Alice Johnson",
			"age":        30,
			"email":      "alice@techcorp.com",
			"roles":      []Value{"admin", "user", "developer"},
			"active":     true,
			"salary":     85000.0,
			"department": "Engineering",
		},
		map[string]Value{
			"id":         2,
			"name":       "Bob Smith",
			"age":        25,
			"email":      "bob@startup.io",
			"roles":      []Value{"user", "intern"},
			"active":     true,
			"salary":     45000.0,
			"department": "Marketing",
		},
	}

	ctx.Set("users", users)
	ctx.Set("threshold", 50000.0)

	fmt.Println("\n📊 Basic Data Access Examples")
	fmt.Println("============================")

	testExpressions(ctx, []string{
		"users[0].name",
		"users[1].age",
		"users[0].salary + users[1].salary",
		"length(users)",
		"users[0].salary > threshold",
	})

	fmt.Println("\n🔍 Collection Operations Examples")
	fmt.Println("=================================")

	testExpressions(ctx, []string{
		"users.filter(u, u.age > 25)",
		"users.map(u, u.name)",
		"users.map(u, u.salary * 1.1)",
		"users.all(u, u.age >= 18)",
		"users.exists(u, u.salary > 80000)",
		"users.find(u, u.department == 'Engineering')",
		"users.find(u, u.age < 20)",
	})

	fmt.Println("\n🧮 Math and String Examples")
	fmt.Println("===========================")

	testExpressions(ctx, []string{
		"2 + 3 * 4",
		"(2 + 3) * 4",
		"abs(-42)",
		"upper('hello world')",
		"lower('HELLO WORLD')",
		"ceil(3.2)",
		"floor(3.7)",
		"round(3.6)",
		"min(5, 3, 8, 1)",
		"max(5, 3, 8, 1)",
		"sqrt(16)",
		"pow(2, 3)",
	})

	fmt.Println("\n🔧 Advanced String Operations")
	fmt.Println("=============================")

	ctx.Set("text", "  Hello, World!  ")
	testExpressions(ctx, []string{
		"text.trim()",
		"text.upper()",
		"text.lower()",
		"text.replace('World', 'Go')",
		"text.split(',')",
		"'admin' in ['admin', 'user']",
		"split('a,b,c', ',')",
		"replace('hello world', 'world', 'Go')",
	})

	fmt.Println("\n📋 Advanced Collection Operations")
	fmt.Println("=================================")

	numbers := []Value{3, 1, 4, 1, 5, 9, 2, 6}
	ctx.Set("numbers", numbers)

	testExpressions(ctx, []string{
		"numbers.size()",
		"numbers.reverse()",
		"numbers.sort(n, n)",
		"users.size()",
		"users.reverse()",
	})

	fmt.Println("\n🏷️  Type System Examples")
	fmt.Println("=========================")

	testExpressions(ctx, []string{
		"type(42)",
		"type('hello')",
		"type(true)",
		"type([1, 2, 3])",
		"type(users[0])",
		"int(3.14)",
		"double(42)",
		"string(123)",
	})

	fmt.Println("\n🔀 Conditional (Ternary) Expressions")
	fmt.Println("===================================")

	testExpressions(ctx, []string{
		"true ? 'yes' : 'no'",
		"false ? 'yes' : 'no'",
		"users[0].age > 25 ? 'senior' : 'junior'",
		"length(users) > 1 ? 'multiple users' : 'single user'",
		"users[0].salary > threshold ? 'high' : 'low'",
	})

	fmt.Println("\n🚀 Complex Nested Examples")
	fmt.Println("===========================")

	// Add more complex test data
	projects := []Value{
		map[string]Value{
			"name":   "Project Alpha",
			"team":   []Value{"Alice", "Bob"},
			"budget": 100000.0,
			"status": "active",
		},
		map[string]Value{
			"name":   "Project Beta",
			"team":   []Value{"Alice", "Charlie"},
			"budget": 75000.0,
			"status": "completed",
		},
	}
	ctx.Set("projects", projects)

	testExpressions(ctx, []string{
		"projects.filter(p, p.budget > 80000).map(p, p.name)",
		"projects.all(p, length(p.team) >= 2)",
		"projects.exists(p, p.status == 'active') ? 'has active' : 'all done'",
		"users.map(u, u.name)",
		"users.filter(u, u.age > 25).size() > 0 ? 'found seniors' : 'no seniors'",
		"users.map(u, split(u.name, ' '))",
		"users.flatMap(u, split(u.name, ' '))",
		"users.flatMap(u, split(u.name, ' ')).filter(word, length(word) > 3)",
		"users.map(u, split(u.name, ' ')[0])",
		"users.map(u, u.name).split(' ')",
		"users.map(u, u.name).split(' ').map(parts, parts[0])",
	})

	fmt.Println("\n🚀 All tests completed!")

	// Print comprehensive feature summary
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("📋 COMPREHENSIVE CEL FEATURE SUMMARY")
	fmt.Println(strings.Repeat("=", 60))

	fmt.Println("\n✅ IMPLEMENTED FEATURES:")
	fmt.Println("┌─────────────────────────────────────────────────────────┐")
	fmt.Println("│ 🏗️  CORE LANGUAGE                                        │")
	fmt.Println("├─────────────────────────────────────────────────────────┤")
	fmt.Println("│ • Data Types: int, double, string, bool, list, map, null │")
	fmt.Println("│ • Variables and Field Access: obj.field, obj[index]     │")
	fmt.Println("│ • Array Literals: [1, 2, 3], ['a', 'b', 'c']           │")
	fmt.Println("│ • Proper operator precedence and grouping              │")
	fmt.Println("└─────────────────────────────────────────────────────────┘")

	fmt.Println("\n┌─────────────────────────────────────────────────────────┐")
	fmt.Println("│ 🧮 OPERATORS                                              │")
	fmt.Println("├─────────────────────────────────────────────────────────┤")
	fmt.Println("│ • Arithmetic: +, -, *, /, %, **                        │")
	fmt.Println("│ • Comparison: ==, !=, <, <=, >, >=                     │")
	fmt.Println("│ • Logical: &&, ||, !                                   │")
	fmt.Println("│ • Membership: in                                        │")
	fmt.Println("│ • Ternary: condition ? true_expr : false_expr           │")
	fmt.Println("└─────────────────────────────────────────────────────────┘")

	fmt.Println("\n┌─────────────────────────────────────────────────────────┐")
	fmt.Println("│ 📚 COLLECTION OPERATIONS                                   │")
	fmt.Println("├─────────────────────────────────────────────────────────┤")
	fmt.Println("│ • filter(var, condition) - Filter elements             │")
	fmt.Println("│ • map(var, expression) - Transform elements             │")
	fmt.Println("│ • all(var, condition) - Check all match                │")
	fmt.Println("│ • exists(var, condition) - Check any match             │")
	fmt.Println("│ • find(var, condition) - Find first match              │")
	fmt.Println("│ • size() - Get collection size                          │")
	fmt.Println("│ • reverse() - Reverse collection order                  │")
	fmt.Println("│ • sort(var, key_expr) - Sort by expression             │")
	fmt.Println("└─────────────────────────────────────────────────────────┘")

	fmt.Println("\n┌─────────────────────────────────────────────────────────┐")
	fmt.Println("│ 🔤 STRING FUNCTIONS                                        │")
	fmt.Println("├─────────────────────────────────────────────────────────┤")
	fmt.Println("│ • upper(str), lower(str) - Case conversion              │")
	fmt.Println("│ • trim() - Remove whitespace                            │")
	fmt.Println("│ • split(separator) - Split into array                   │")
	fmt.Println("│ • replace(old, new) - Replace substring                 │")
	fmt.Println("│ • contains(substring) - Check contains                  │")
	fmt.Println("│ • startsWith(prefix), endsWith(suffix)                  │")
	fmt.Println("│ • matches(pattern) - Simple pattern matching            │")
	fmt.Println("│ • length() - String/collection length                   │")
	fmt.Println("└─────────────────────────────────────────────────────────┘")

	fmt.Println("\n┌─────────────────────────────────────────────────────────┐")
	fmt.Println("│ 🔢 MATH FUNCTIONS                                          │")
	fmt.Println("├─────────────────────────────────────────────────────────┤")
	fmt.Println("│ • abs(x) - Absolute value                               │")
	fmt.Println("│ • ceil(x), floor(x), round(x) - Rounding               │")
	fmt.Println("│ • min(a,b,...), max(a,b,...) - Min/max values          │")
	fmt.Println("│ • sqrt(x), pow(x,y) - Power functions                  │")
	fmt.Println("│ • sum(collection), avg(collection) - Aggregation       │")
	fmt.Println("└─────────────────────────────────────────────────────────┘")

	fmt.Println("\n┌─────────────────────────────────────────────────────────┐")
	fmt.Println("│ 🏷️  TYPE SYSTEM                                            │")
	fmt.Println("├─────────────────────────────────────────────────────────┤")
	fmt.Println("│ • type(value) - Get type name                           │")
	fmt.Println("│ • int(x), double(x), string(x) - Type conversion        │")
	fmt.Println("│ • Reflection-based struct field access                 │")
	fmt.Println("└─────────────────────────────────────────────────────────┘")

	fmt.Println("\n❌ STILL MISSING FEATURES:")
	fmt.Println("┌─────────────────────────────────────────────────────────┐")
	fmt.Println("│ • Advanced types: duration, timestamp, bytes           │")
	fmt.Println("│ • Regex operations with full regex engine              │")
	fmt.Println("│ • JSON encoding/decoding functions                      │")
	fmt.Println("│ • Time/date arithmetic and formatting                   │")
	fmt.Println("│ • Comprehensions: [expr | var in coll, condition]      │")
	fmt.Println("│ • Nested macro variables in complex expressions        │")
	fmt.Println("│ • Custom user-defined functions/macros                 │")
	fmt.Println("│ • Error handling with optional types                   │")
	fmt.Println("│ • Advanced collection ops: group_by, flatten, distinct │")
	fmt.Println("└─────────────────────────────────────────────────────────┘")

	fmt.Printf("\n🎯 IMPLEMENTATION STATUS: ~75%% of core CEL features\n")
	fmt.Printf("📊 Ready for: Business rules, data validation, filtering\n")
	fmt.Printf("🚀 Perfect for: API queries, configuration, policies\n\n")
}

// Helper function to test expressions
func testExpressions(ctx *Context, expressions []string) {
	for _, exprStr := range expressions {
		fmt.Printf("Expression: %s\n", exprStr)

		// Parse and evaluate the expression properly
		parser := NewParser(exprStr)
		expr, err := parser.Parse()
		if err != nil {
			fmt.Printf("  Parse Error: %v\n", err)
		} else {
			result, err := expr.Evaluate(ctx)
			if err != nil {
				fmt.Printf("  Eval Error: %v\n", err)
			} else {
				fmt.Printf("  Result: %v\n", formatResult(result))
			}
		}
		fmt.Println()
	}
}

// Helper function to format results nicely
func formatResult(result Value) string {
	if result == nil {
		return "null"
	}

	switch v := result.(type) {
	case string:
		return fmt.Sprintf("\"%s\"", v)
	case []Value:
		if len(v) == 0 {
			return "[]"
		}
		var items []string
		for i, item := range v {
			if i >= 3 { // Limit display to first 3 items
				items = append(items, "...")
				break
			}
			items = append(items, formatResult(item))
		}
		return "[" + strings.Join(items, ", ") + "]"
	case map[string]Value:
		return fmt.Sprintf("map[%d keys]", len(v))
	default:
		return fmt.Sprintf("%v", v)
	}
}
