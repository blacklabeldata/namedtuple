package schema

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Token Type enum
type tokenType uint8

// Lex items
const (
	tokenError             tokenType = iota // Lexer Error
	tokenEOF                                // End of File
	tokenComment                            // Comment
	tokenMessage                            // Message keyword
	tokenVersion                            // Version keyword
	tokenValueType                          // Value type ID (string, uint8)
	tokenRequired                           // Required Keyword
	tokenOptional                           // Optional Keyword
	tokenVersionNumber                      // Version ID
	tokenOpenCurlyBracket                   // Left {
	tokenCloseCurlyBracket                  // Right }
	tokenOpenArrayBracket                   // Open Array [
	tokenCloseArrayBracket                  // Close Array ]
	tokenEquals                             // Equals sign
	tokenIdentifier                         // Message or Field Name
	tokenReference                          // Message type reference
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
var types = []string{"string",
	"uint8", "int8",
	"uint16", "int16",
	"uint32", "int32",
	"uint64", "int64",
	"float32", "float64", "date",
	"tuple", "any",
}

// Token struct
type token struct {
	typ tokenType // Type, such as itemNumber
	val string    // Value, such as "23.2"
}

// Used to print tokens
func (t token) String() string {
	switch t.typ {
	case tokenEOF:
		return "EOF"
	case tokenError:
		return t.val
	}
	if len(t.val) > 10 {
		return fmt.Sprintf("%.10q...", t.val)
	}
	return fmt.Sprintf("%q", t.val)
}

// stateFn represents the state of the scanner
// as a function and returns the next state.
type stateFn func(*lexer) stateFn

// lex creates a new scanner from the input
func lex(name, input string) *lexer {
	return &lexer{
		name:   name,
		input:  input,
		state:  lexText,
		tokens: make(chan token, 2),
	}
}

// lexer holds the state of the scanner
type lexer struct {
	name   string     // Used to error reports
	input  string     // the string being scanned
	start  int        // start position of this item
	pos    int        // current position in the input
	width  int        // width of last rune read
	state  stateFn    // next state function
	tokens chan token // channel of scanned tokens
}

// Run lexes the input by executing state functions
// until the state is nil
func (l *lexer) run() {
	for state := lexText; state != nil; {
		state = state(l)
	}
	close(l.tokens) // no more tokens will be delivered
}

// emit passes an item pack to the client
func (l *lexer) emit(t tokenType) {
	l.tokens <- token{t, l.input[l.start:l.pos]}
	l.start = l.pos
}

func (l *lexer) remaining() string {
	return l.input[l.pos:]
}

func (l *lexer) skipWhitespace() {
	l.acceptRun(" \t\r\n")
	l.ignore()
}

func (l *lexer) next() (r rune) {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}
	r, l.width = utf8.DecodeRuneInString(l.remaining())
	l.pos += l.width
	return
}

// ignore steps over the pending input before this point
func (l *lexer) ignore() {
	l.start = l.pos
}

// backup steps back one rune
func (l *lexer) backup() {
	l.pos -= l.width
}

// peek returns but does not consume the next rune in the input
func (l *lexer) peek() (r rune) {
	r = l.next()
	l.backup()
	return
}

func (l *lexer) nextToken() token {
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
func (l *lexer) accept(valid string) bool {
	if strings.IndexRune(valid, l.next()) >= 0 {
		return true
	}
	l.backup()
	return false
}

// consumes a run of runes from the valid set
func (l *lexer) acceptRun(valid string) {
	for strings.IndexRune(valid, l.next()) >= 0 {
	}
	l.backup()
}

// errorf returns an error token and terminates the scan
// by passing back a nil pointer that will be the next
// state thus terminating the lexer
func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.tokens <- token{tokenError, fmt.Sprintf(format, args...)}
	return nil
}

// Main lexer loop
func lexText(l *lexer) stateFn {
	for {
		l.skipWhitespace()

		if strings.HasPrefix(l.remaining(), comment) { // Start comment
			if l.pos > l.start {
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
			l.emit(tokenCloseCurlyBracket)
		} else {
			switch r := l.next(); {

			case r == eof: // reached EOF?
				l.emit(tokenEOF)
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
func lexComment(l *lexer) stateFn {
	l.skipWhitespace()

	for strings.HasPrefix(l.remaining(), comment) {
		// skip comment //
		l.pos += len(comment)

		// find next new line and add location to pos which
		// advances the scanner
		if index := strings.Index(l.remaining(), "\n"); index > 0 {
			l.pos += index
		}

		// emit the comment string
		l.emit(tokenComment)

		l.skipWhitespace()
	}

	// continue on scanner
	return lexText
}

func lexMessage(l *lexer) stateFn {
	// skip message keyword
	l.pos += len(message)

	// emit keyword
	l.emit(tokenMessage)

	for strings.HasPrefix(l.remaining(), " ") {
		l.ignore()
	}
	return lexIdentifier
}

func lexIdentifier(l *lexer) stateFn {
	l.skipWhitespace()

	for {

		if strings.HasPrefix(l.remaining(), openScope) {
			l.emit(tokenIdentifier)
			return lexMessageBody
		}

		if l.next() == eof {
			l.emit(tokenEOF)
			return nil
		}

		// switch r := l.next(); {
		// case unicode.IsSpace(r):
		// 	l.ignore()
		// case unicode.IsLetter(r) || unicode.IsDigit(r):
		// 	insideId = true
		// case r == '.' || r == '-' || r == '_':
		// }
	}
}

// func lexIdentifier(l *lexer) stateFn {

// 	// find open bracket
// 	if index := strings.IndexAny(l.input[l.pos:], "{"); index > 0 {

// 		// update pos without open bracket
// 		l.pos += (index - 1)

// 		// emit identifier
// 		l.emit(tokenIdentifier)

// 		// lex message contents
// 		return lexMessageBody
// 	}
// 	return l.errorf("missing message body")
// }

func lexMessageBody(l *lexer) stateFn {
	l.pos += len(openScope)
	l.emit(tokenOpenCurlyBracket)
	return lexText
}

func lexVersion(l *lexer) stateFn {
	l.pos += len(version)
	l.emit(tokenVersion)
	l.skipWhitespace()

	l.acceptRun("0123456789")
	l.emit(tokenVersionNumber)
	l.skipWhitespace()

	if strings.HasPrefix(l.remaining(), openScope) {
		l.pos += len(openScope)
		l.emit(tokenOpenCurlyBracket)
	} else {
		return l.errorf("missing version body")
	}
	return lexText
}

func lexField(l *lexer) stateFn {
	l.skipWhitespace()
	lexComment(l)

	if strings.HasPrefix(l.remaining(), required) {
		l.pos += len(required)
		l.emit(tokenRequired)
	} else if strings.HasPrefix(l.remaining(), optional) {
		l.pos += len(optional)
		l.emit(tokenOptional)
	} else if strings.HasPrefix(l.remaining(), closeScope) {
		l.pos += len(closeScope)
		l.emit(tokenCloseCurlyBracket)
		return lexText
	} else {
		return l.errorf("expected 'required' or 'optional'")
	}

	l.skipWhitespace()
	lexIdentifier(l)
	l.skipWhitespace()
	if l.accept("=") {
		l.emit(tokenEquals)
	} else {
		return l.errorf("expected '=' sign")
	}
	l.skipWhitespace()

	lexType(l)
	return lexField
}

func lexType(l *lexer) stateFn {
	l.skipWhitespace()
	if strings.HasPrefix(l.remaining(), openArray) {
		l.pos += len(openArray)
		l.emit(tokenOpenArrayBracket)

		l.skipWhitespace()
		lexType(l)
		l.skipWhitespace()

		if strings.HasPrefix(l.remaining(), closeArray) {
			l.pos += len(closeArray)
			l.emit(tokenCloseArrayBracket)
			return lexField
		} else {
			return l.errorf("expected ]")
		}
	} else if strings.HasPrefix(l.remaining(), dollarRef) {
		l.emit(tokenReference)
		lexIdentifier(l)
		return lexField
	} else {
		for _, t := range types {
			if strings.HasPrefix(l.remaining(), t) {
				l.emit(tokenValueType)
				return lexField
			}
		}
	}
	return l.errorf("expected type name, reference or array type")
}
