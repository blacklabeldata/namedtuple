package schema

import (
	"fmt"
	"testing"
)

func TestLoop(t *testing.T) {

	text := `// this is a comment`

	l := lex("tuple", text)
	// go l.run()
	for tok := l.nextToken(); tok.typ != tokenEOF; {
		fmt.Println(tok)
	}
}
