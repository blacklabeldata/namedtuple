package schema

import (
    // "fmt"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "testing"

    "github.com/stretchr/testify/assert"
    // "time"
)

func TestComment(t *testing.T) {
    text := `
    // this is a comment
    `

    var token Token
    l := NewLexer("tuple", text, func(t Token) {
        token = t
    })

    // lex comment
    lexComment(l)

    // consume token
    // token := <-l.tokens

    // expecting comment token
    assert.Equal(t, TokenComment, token.Type)

    // validate text
    assert.Equal(t, "// this is a comment", token.Value)
}

func TestMultiLineComment(t *testing.T) {
    text := `
    // this is a comment
    // This is also a comment
    // This is one too
    `

    var token Token
    l := NewLexer("tuple", text, func(t Token) {
        // fmt.Println("handler: ", t)
        token = t
    })

    // lex comment
    lexComment(l)

    // expecting comment token and validating text
    assert.Equal(t, TokenComment, token.Type)
    assert.Equal(t, "// this is a comment", token.Value)

    // lex second comment
    lexComment(l)

    // expecting comment token and validating text
    assert.Equal(t, TokenComment, token.Type)
    assert.Equal(t, "// This is also a comment", token.Value)

    // lex third comment
    lexComment(l)

    // expecting comment token and validating text
    assert.Equal(t, TokenComment, token.Type)
    assert.Equal(t, "// This is one too", token.Value)
}

func TestLoop(t *testing.T) {
    text := `
    // this is a comment
    // This is also a comment
    // This is one too
    type User {
        // version comment
        version 1 {
            required string uuid
            required string username
            optional uint8 age
        }

        // 11/15/14
        version 2 {
            optional Location location
        }
    }
    `

    var token Token
    l := NewLexer("tuple", text, func(t Token) {
        fmt.Println("handler: ", t.Type, t)
        token = t
    })
    // lexText(l)
    //
    // var start = time.Now()
    l.run()
    // fmt.Println(time.Now().Sub(start).Seconds())
}

func TestComplexFile(t *testing.T) {
    // filename, err := filepath.Abs("./examples/complex.ent")
    file, err := os.Open("./examples/complex.ent") // For read access.
    if err != nil {
        log.Fatal(err)
    }
    bytes, err := ioutil.ReadAll(file)
    text := string(bytes)

    l := NewLexer("complex file", text, func(t Token) {
        fmt.Printf("%#v\n", t)
    })
    l.run()
}

// func TestLoop2(t *testing.T) {
//  text := `
//     // this is a comment
//     // This is also a comment
//     // This is one too
//     message
//     `

//  // var token Token
//  var tokens = make(chan Token, 2)
//  l := NewLexer("tuple", text, func(t Token) {
//      // fmt.Println("handler: ", t)
//      // token = t
//      tokens <- t
//      if t.Type == TokenEOF {
//          close(tokens)
//      }
//  })
//  // lexText(l)
//  //
//  go l.run()
//  var start = time.Now()
//  for token := range tokens {
//      fmt.Println("handler: ", token)
//  }
//  fmt.Println(time.Now().Sub(start).Seconds())
// }
