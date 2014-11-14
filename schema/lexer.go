package schema

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Token Type enum
type TokenType uint8

// Lex items
const (
	TokenError             TokenType = iota // Lexer Error
	TokenEOF                                // End of File
	TokenComment                            // Comment
	TokenMessage                            // Message keyword
	TokenVersion                            // Version keyword
	TokenValueType                          // Value type ID (string, uint8)
	TokenRequired                           // Required Keyword
	TokenOptional                           // Optional Keyword
	TokenVersionNumber                      // Version ID
	TokenOpenCurlyBracket                   // Left {
	TokenCloseCurlyBracket                  // Right }
	TokenOpenArrayBracket                   // Open Array [
	TokenCloseArrayBracket                  // Close Array ]
	TokenEquals                             // Equals sign
	TokenIdentifier                         // Message or Field Name
	TokenReference                          // Message type reference
)

// Constant Punctuation and Keywords
const (
	openScope  = "{"
	closeScope = "}"
	openArray  = "["
	closeArray = "]"
	equals     = "="
	comment    = "//"
	dollarRef  = "$"
	message    = "message"
	version    = "version"
	required   = "required"
	optional   = "optional"
)

const eof = -1

// Types
var TypeNames = []string{"string",
	"uint8", "int8",
	"uint16", "int16",
	"uint32", "int32",
	"uint64", "int64",
	"float32", "float64", "date",
	"tuple", "any",
}

// Token struct
type Token struct {
	Type  TokenType // Type, such as itemNumber
	Value string    // Value, such as "23.2"
}

// Used to print tokens
func (t Token) String() string {
	switch t.Type {
	case TokenEOF:
		return "EOF"
	case TokenError:
		return t.Value
	}
	if len(t.Value) > 10 {
		return fmt.Sprintf("%.10q...", t.Value)
	}
	return fmt.Sprintf("%q", t.Value)
}

// stateFn represents the state of the scanner
// as a function and returns the next state.
type stateFn func(*Lexer) stateFn

// NewLexer creates a new scanner from the input
func NewLexer(name, input string) *Lexer {
	return &Lexer{
		Name:   name,
		input:  input,
		state:  lexText,
		tokens: make(chan Token, 2),
	}
}

// Lexer holds the state of the scanner
type Lexer struct {
	Name   string     // Used to error reports
	input  string     // the string being scanned
	Start  int        // start position of this item
	Pos    int        // current position in the input
	Width  int        // width of last rune read
	state  stateFn    // next state function
	tokens chan Token // channel of scanned tokens
}

// Run lexes the input by executing state functions
// until the state is nil
func (l *Lexer) run() {
	for state := lexText; state != nil; {
		state = state(l)
	}
	close(l.tokens) // no more tokens will be delivered
}

// emit passes an item pack to the client
func (l *Lexer) emit(t TokenType) {
	l.tokens <- Token{t, l.input[l.Start:l.Pos]}
	l.Start = l.Pos
}

func (l *Lexer) remaining() string {
	return l.input[l.Pos:]
}

func (l *Lexer) skipWhitespace() {
	l.acceptRun(" \t\r\n")
	l.ignore()
}

func (l *Lexer) next() (r rune) {
	if l.Pos >= len(l.input) {
		l.Width = 0
		return eof
	}
	r, l.Width = utf8.DecodeRuneInString(l.remaining())
	l.Pos += l.Width
	return
}

// ignore steps over the pending input before this point
func (l *Lexer) ignore() {
	l.Start = l.Pos
}

// backup steps back one rune
func (l *Lexer) backup() {
	l.Pos -= l.Width
}

// peek returns but does not consume the next rune in the input
func (l *Lexer) peek() (r rune) {
	r = l.next()
	l.backup()
	return
}

func (l *Lexer) NextToken() Token {
	for {
		select {
		case item := <-l.tokens:
			return item
		default:
			l.state = l.state(l)
		}
	}
	panic("not reached")
}

// accept consumes the next rune
// if it's in the valid set
func (l *Lexer) accept(valid string) bool {
	if strings.IndexRune(valid, l.next()) >= 0 {
		return true
	}
	l.backup()
	return false
}

// consumes a run of runes from the valid set
func (l *Lexer) acceptRun(valid string) {
	for strings.IndexRune(valid, l.next()) >= 0 {
	}
	l.backup()
}

// errorf returns an error token and terminates the scan
// by passing back a nil pointer that will be the next
// state thus terminating the lexer
func (l *Lexer) errorf(format string, args ...interface{}) stateFn {
	l.tokens <- Token{TokenError, fmt.Sprintf(format, args...)}
	return nil
}

// Main lexer loop
func lexText(l *Lexer) stateFn {
	for {
		l.skipWhitespace()

		if strings.HasPrefix(l.remaining(), comment) { // Start comment
			if l.Pos > l.Start {
				return lexComment // state function which lexes a comment
			}
		} else if strings.HasPrefix(l.remaining(), message) { // Start message
			return lexMessage // state function which lexes a message
		} else if strings.HasPrefix(l.remaining(), version) { // Start version
			return lexVersion // state function which lexes a version
		} else if strings.HasPrefix(l.remaining(), required) { // Start required field
			return lexField // state function which lexes a field
		} else if strings.HasPrefix(l.remaining(), optional) { // Start optional field
			return lexField // state function which lexes a field
		} else if strings.HasPrefix(l.remaining(), closeScope) { // Close scope
			l.emit(TokenCloseCurlyBracket)
		} else {
			switch r := l.next(); {

			case r == eof: // reached EOF?
				l.emit(TokenEOF)
				break
			default:
				l.errorf("unknown token")
			}
		}
	}

	// Stops the run loop
	return nil
}

// Lexes a comment line
func lexComment(l *Lexer) stateFn {
	l.skipWhitespace()

	for strings.HasPrefix(l.remaining(), comment) {
		// skip comment //
		l.Pos += len(comment)

		// find next new line and add location to pos which
		// advances the scanner
		if index := strings.Index(l.remaining(), "\n"); index > 0 {
			l.Pos += index
		}

		// emit the comment string
		l.emit(TokenComment)

		l.skipWhitespace()
	}

	// continue on scanner
	return lexText
}

func lexMessage(l *Lexer) stateFn {
	// skip message keyword
	l.Pos += len(message)

	// emit keyword
	l.emit(TokenMessage)

	for strings.HasPrefix(l.remaining(), " ") {
		l.ignore()
	}
	return lexIdentifier
}

func lexIdentifier(l *Lexer) stateFn {
	l.skipWhitespace()

	for {

		if strings.HasPrefix(l.remaining(), openScope) {
			l.emit(TokenIdentifier)
			return lexMessageBody
		}

		if l.next() == eof {
			l.emit(TokenEOF)
			return nil
		}

		// switch r := l.next(); {
		// case unicode.IsSpace(r):
		//  l.ignore()
		// case unicode.IsLetter(r) || unicode.IsDigit(r):
		//  insideId = true
		// case r == '.' || r == '-' || r == '_':
		// }
	}
}

// func lexIdentifier(l *Lexer) stateFn {

//  // find open bracket
//  if index := strings.IndexAny(l.input[l.Pos:], "{"); index > 0 {

//      // update pos without open bracket
//      l.Pos += (index - 1)

//      // emit identifier
//      l.emit(TokenIdentifier)

//      // lex message contents
//      return lexMessageBody
//  }
//  return l.errorf("missing message body")
// }

func lexMessageBody(l *Lexer) stateFn {
	l.Pos += len(openScope)
	l.emit(TokenOpenCurlyBracket)
	return lexText
}

func lexVersion(l *Lexer) stateFn {
	l.Pos += len(version)
	l.emit(TokenVersion)
	l.skipWhitespace()

	l.acceptRun("0123456789")
	l.emit(TokenVersionNumber)
	l.skipWhitespace()

	if strings.HasPrefix(l.remaining(), openScope) {
		l.Pos += len(openScope)
		l.emit(TokenOpenCurlyBracket)
	} else {
		return l.errorf("missing version body")
	}
	return lexText
}

func lexField(l *Lexer) stateFn {
	l.skipWhitespace()
	lexComment(l)

	if strings.HasPrefix(l.remaining(), required) {
		l.Pos += len(required)
		l.emit(TokenRequired)
	} else if strings.HasPrefix(l.remaining(), optional) {
		l.Pos += len(optional)
		l.emit(TokenOptional)
	} else if strings.HasPrefix(l.remaining(), closeScope) {
		l.Pos += len(closeScope)
		l.emit(TokenCloseCurlyBracket)
		return lexText
	} else {
		return l.errorf("expected 'required' or 'optional'")
	}

	l.skipWhitespace()
	lexIdentifier(l)
	l.skipWhitespace()
	if l.accept("=") {
		l.emit(TokenEquals)
	} else {
		return l.errorf("expected '=' sign")
	}
	l.skipWhitespace()

	lexType(l)
	return lexField
}

func lexType(l *Lexer) stateFn {
	l.skipWhitespace()
	if strings.HasPrefix(l.remaining(), openArray) {
		l.Pos += len(openArray)
		l.emit(TokenOpenArrayBracket)

		l.skipWhitespace()
		lexType(l)
		l.skipWhitespace()

		if strings.HasPrefix(l.remaining(), closeArray) {
			l.Pos += len(closeArray)
			l.emit(TokenCloseArrayBracket)
			return lexField
		} else {
			return l.errorf("expected ]")
		}
	} else if strings.HasPrefix(l.remaining(), dollarRef) {
		l.emit(TokenReference)
		lexIdentifier(l)
		return lexField
	} else {
		for _, t := range TypeNames {
			if strings.HasPrefix(l.remaining(), t) {
				l.emit(TokenValueType)
				return lexField
			}
		}
	}
	return l.errorf("expected type name, reference or array type")
}
