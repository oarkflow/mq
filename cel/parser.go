package cel

import (
	"fmt"
	"strconv"
	"strings"
)

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
	if p.peekToken.Type == RBRACKET {
		p.nextToken()
		return &ArrayLiteral{}
	}

	p.nextToken()

	// Check for list comprehension: [expr | var in collection, condition]
	firstExpr := p.parseExpression(LOWEST)

	if p.peekToken.Type == PIPE {
		// This is a list comprehension
		p.nextToken() // consume the '|'

		if !p.expectPeek(IDENTIFIER) {
			return nil
		}
		variable := p.curToken.Literal

		if !p.expectPeek(IN) {
			return nil
		}

		p.nextToken()
		collection := p.parseExpression(LOWEST)

		var condition Expression
		if p.peekToken.Type == COMMA {
			p.nextToken() // consume comma
			p.nextToken()
			condition = p.parseExpression(LOWEST)
		}

		if !p.expectPeek(RBRACKET) {
			return nil
		}

		return &Comprehension{
			Expression: firstExpr,
			Variable:   variable,
			Collection: collection,
			Condition:  condition,
		}
	}

	// Regular array literal
	array := &ArrayLiteral{}
	array.Elements = append(array.Elements, firstExpr)

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
