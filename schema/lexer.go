package schema

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// TokenType enum
type TokenType uint8

// Lex items
const (
	TokenError             TokenType = iota // 0 Lexer Error
	TokenEOF                                // 1 End of File
	TokenComment                            // 2 Comment
	TokenTypeDef                            // 3 Type keyword
	TokenVersion                            // 4 Version keyword
	TokenValueType                          // 5 Value type ID (string, uint8)
	TokenRequired                           // 6 Required Keyword
	TokenOptional                           // 7 Optional Keyword
	TokenVersionNumber                      // 8 Version ID
	TokenOpenCurlyBracket                   // 9 Left {
	TokenCloseCurlyBracket                  // 10 Right }
	TokenOpenArrayBracket                   // 11 Open Array [
	TokenCloseArrayBracket                  // 12 Close Array ]
	TokenEquals                             // 13 Equals sign
	TokenIdentifier                         // 14 Message or Field Name
	TokenReference                          // 15 Message type reference
	TokenComma                              // 16 Comma
	TokenPeriod                             // 17 Period
	TokenNamespace                          // 18 Namespace keyword
	TokenImport                             // 19 Import keyword
	TokenFrom                               // 20 From keyword
	TokenAs                                 // 21 As keyword
	TokenPackage                            // 22 Package keyword
	TokenPackageName                        // 23 Package name
	TokenAsterisk                           // 24 Package all
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
	typeDef    = "type"
	version    = "version"
	required   = "required"
	optional   = "optional"
	period     = "."
	comma      = ","
	from       = "from"
	imp        = "import"
	as         = "as"
	pkg        = "package"
	asterisk   = "*"
)

// eof represents the end of file/input
const eof = -1

// Reserved Types
var TypeNames = []string{"string", "byte",
	"uint8", "int8",
	"uint16", "int16",
	"uint32", "int32",
	"uint64", "int64",
	"float32", "float64", "timestamp",
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
		return fmt.Sprintf("%.25q...", t.Value)
	}
	return fmt.Sprintf("%q", t.Value)
}

// Handler is simpley a function which takes a single Token argument
type Handler func(Token)

// stateFn represents the state of the scanner
// as a function and returns the next state.
type stateFn func(*Lexer) stateFn

// NewLexer creates a new scanner from the input
func NewLexer(name, input string, h Handler) *Lexer {
	return &Lexer{
		Name:    name,
		input:   input + "\n",
		state:   lexText,
		handler: h,
		// tokens: make(chan Token, 2),
	}
}

// Lexer holds the state of the scanner
type Lexer struct {
	Name    string  // Used to error reports
	input   string  // the string being scanned
	Start   int     // start position of this item
	Pos     int     // current position in the input
	Width   int     // width of last rune read
	state   stateFn // next state function
	handler Handler // token handler
	// tokens chan Token // channel of scanned tokens
}

// Run lexes the input by executing state functions
// until the state is nil
func (l *Lexer) run() {
	for state := lexText; state != nil; {
		state = state(l)
	}
	// close(l.tokens) // no more tokens will be delivered
}

// emit passes an item pack to the client
func (l *Lexer) emit(t TokenType) {

	// if the position is the same as the start, do not emit a token
	if l.Pos == l.Start {
		return
	}

	tok := Token{t, l.input[l.Start:l.Pos]}
	// fmt.Println("token: ", tok)
	l.handler(tok)
	// l.tokens <- Token{t, l.input[l.Start:l.Pos]}
	l.Start = l.Pos
}

// remaining returns the input from the current position until the end
func (l *Lexer) remaining() string {
	return l.input[l.Pos:]
}

// advance increases the current position by the given amount
func (l *Lexer) advance(incr int) {
	l.Pos += incr
	return
}

// skipWhitespace ignores all whitespace characters
func (l *Lexer) skipWhitespace() {
	l.acceptRun(" \t\r\n")
	l.ignore()
}

// next advances the lexer position and returns the next rune. If the input
// does not have any more runes, an `eof` is returned.
func (l *Lexer) next() (r rune) {
	if l.Pos >= len(l.input) {
		l.Width = 0
		return eof
	}
	r, l.Width = utf8.DecodeRuneInString(l.remaining())
	l.advance(l.Width)
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

// accept consumes the next rune if it's in the valid set
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
	l.handler(Token{TokenError, fmt.Sprintf(l.Name+": "+format, args...)})
	// l.tokens <- Token{TokenError, fmt.Sprintf(format, args...)}
	return nil
}

// Main lexer loop
func lexText(l *Lexer) stateFn {
OUTER:
	for {
		l.skipWhitespace()

		if strings.HasPrefix(l.remaining(), comment) { // Start comment
			// state function which lexes a comment
			return lexComment
		} else if strings.HasPrefix(l.remaining(), pkg) { // Start package decl
			// state function which lexes a package decl
			return lexPackage
		} else if strings.HasPrefix(l.remaining(), from) { // Start from decl
			// state function which lexes a from decl
			return lexFrom
		} else if strings.HasPrefix(l.remaining(), typeDef) { // Start type def
			// state function which lexes a type
			return lexTypeDef
		} else if strings.HasPrefix(l.remaining(), version) { // Start version
			// state function which lexes a version
			return lexVersion
		} else if strings.HasPrefix(l.remaining(), required) { // Start required field
			// state function which lexes a field
			l.Pos += len(required)
			l.emit(TokenRequired)
			l.skipWhitespace()

			return lexType
		} else if strings.HasPrefix(l.remaining(), optional) { // Start optional field
			// state function which lexes a field
			l.Pos += len(optional)
			l.emit(TokenOptional)
			l.skipWhitespace()
			return lexType
		} else if strings.HasPrefix(l.remaining(), openScope) { // Open scope
			l.Pos += len(openScope)
			l.emit(TokenOpenCurlyBracket)
		} else if strings.HasPrefix(l.remaining(), closeScope) { // Close scope
			l.Pos += len(closeScope)
			l.emit(TokenCloseCurlyBracket)
		} else {
			switch r := l.next(); {

			case r == eof: // reached EOF?
				l.emit(TokenEOF)
				break OUTER
			default:
				l.errorf("unknown token: %#v", string(r))
			}
		}
	}

	// Stops the run loop
	return nil
}

// Lexes a comment line
func lexComment(l *Lexer) stateFn {
	l.skipWhitespace()

	// if strings.HasPrefix(l.remaining(), comment) {
	// skip comment //
	l.Pos += len(comment)

	// find next new line and add location to pos which
	// advances the scanner
	if index := strings.Index(l.remaining(), "\n"); index > 0 {
		l.Pos += index
	} else {
		l.Pos += len(l.remaining())
		// l.emit(TokenComment)
		// break
	}

	// emit the comment string
	l.emit(TokenComment)

	l.skipWhitespace()
	// }

	// continue on scanner
	return lexText
}

func lexTypeDef(l *Lexer) stateFn {
	// skip type keyword
	l.Pos += len(typeDef)

	// emit keyword
	l.emit(TokenTypeDef)
	l.skipWhitespace()

	return lexIdentifier(l, lexText, false)
}

func lexLetters(l *Lexer, t TokenType) bool {

OUTER:
	for {
		switch r := l.next(); {
		case unicode.IsLetter(r):
		default:
			l.backup()
			break OUTER
		}
	}

	if l.Pos == l.Start {
		return false
	}

	// emit token
	l.emit(t)
	return true
}

func lexIdentifier(l *Lexer, next stateFn, allowMultiple bool) stateFn {
	l.skipWhitespace()

	// look for identifier
	if !lexLetters(l, TokenIdentifier) {
		return l.errorf("expected identifier")
	}

	// check for second identifier
	if allowMultiple && l.peek() == ',' {
		l.advance(len(comma))
		l.emit(TokenComma)

		// lex identifier
		return lexIdentifier(l, next, allowMultiple)
	}

	return next
}

func lexPackage(l *Lexer) stateFn {
	// skip package keyword
	l.Pos += len(pkg)

	// emit package token
	l.emit(TokenPackage)

	// skip whitespace
	l.skipWhitespace()

	// lex package name
	return lexPackageName
}

func lexPackageName(l *Lexer) stateFn {

	// lex package name
	var lastPeriod bool
OUTER:
	for {

		switch r := l.next(); {
		case unicode.IsLetter(r):
			lastPeriod = false
		case r == '.' || r == '_':
			lastPeriod = true
		case unicode.Is(unicode.White_Space, r):
			l.backup()
			break OUTER
		default:
			l.backup()
			lastPeriod = false
			return l.errorf("expected newline after package name")
		}
	}

	if lastPeriod {
		return l.errorf("package names cannot end with a period or underscore")
	}

	// emit package name
	l.emit(TokenPackageName)

	return lexText
}

func lexFrom(l *Lexer) stateFn {

	// skip package keyword
	l.Pos += len(from)

	// emit from token
	l.emit(TokenFrom)

	// skip whitespace
	l.skipWhitespace()

	// lex package name
	lexPackageName(l)

	// lex import statement
	return lexImport
}

func lexImport(l *Lexer) stateFn {
	l.skipWhitespace()

	// skip package keyword
	l.Pos += len(imp)

	// emit from token
	l.emit(TokenImport)

	// skip whitespace
	l.skipWhitespace()

	if l.peek() == '*' {
		l.next()

		// package all
		l.emit(TokenAsterisk)
	} else {
		// lex type name
		var lastComma bool
	OUTER:
		for {

			switch r := l.next(); {
			case unicode.IsLetter(r):
				lastComma = false
			case r == ',':

				// backup before comma
				l.backup()

				// emit type name
				l.emit(TokenIdentifier)

				// skip comma
				l.next()
				l.ignore()
				lastComma = true

			case r == '\n':
				l.backup()
				break OUTER
			case unicode.Is(unicode.White_Space, r):
				l.skipWhitespace()
			default:
				l.backup()
				lastComma = false
				return l.errorf("expected newline after package name")
			}
		}

		if lastComma {
			return l.errorf("package names cannot end with a comma")
		}

		// emit last type name
		l.emit(TokenIdentifier)

	}

	// lex package name
	return lexText
}

func lexVersion(l *Lexer) stateFn {
	l.Pos += len(version)
	l.emit(TokenVersion)
	l.skipWhitespace()

	l.acceptRun("0123456789")
	if l.Start != l.Pos {
		l.emit(TokenVersionNumber)
	}

	return lexText
}

func lexType(l *Lexer) stateFn {

	if strings.HasPrefix(l.remaining(), openArray) {
		l.advance(len(openArray))
		l.emit(TokenOpenArrayBracket)

		if !strings.HasPrefix(l.remaining(), closeArray) {
			return l.errorf("expected ]")
		}
		l.advance(len(closeArray))
		l.emit(TokenCloseArrayBracket)
	}

	// try built in types
	var foundType bool
	for _, t := range TypeNames {
		if strings.HasPrefix(l.remaining(), t) {
			l.advance(len(t))
			l.emit(TokenValueType)
			foundType = true
			break
		}
	}

	// if it's not a built in type
	if !foundType {

		// look for type name
		if !lexLetters(l, TokenValueType) {
			return l.errorf("expected identifier")
		}
	}

	return lexIdentifier(l, lexText, true)
	// return l.errorf("expected type name, reference or array type")
}
