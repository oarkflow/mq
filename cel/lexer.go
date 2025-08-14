package cel

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
			tok = Token{Type: PIPE, Literal: string(l.ch), Position: l.position}
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
