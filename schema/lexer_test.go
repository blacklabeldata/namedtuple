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

func TestPackageParsing(t *testing.T) {

    text := `package users`
    var tokens []Token
    l := NewLexer("tuple", text, func(t Token) {
        // fmt.Println("handler: ", t)
        tokens = append(tokens, t)
    })

    // lex content
    l.run()

    // there should be 2 tokens
    assert.Equal(t, len(tokens), 2)

    // expecting package token and validating text
    assert.Equal(t, TokenPackage, tokens[0].Type)
    assert.Equal(t, "package", tokens[0].Value)

    // expecting package name token and validating text
    assert.Equal(t, TokenPackageName, tokens[1].Type)
    assert.Equal(t, "users", tokens[1].Value)
}

func TestImportParsing(t *testing.T) {

    text := `from project.users import User, Gadget, Widget`
    // var token Token
    var tokens []Token
    l := NewLexer("tuple", text, func(t Token) {
        // fmt.Println("handler: ", t)
        // token = t
        tokens = append(tokens, t)
    })

    // lex content
    l.run()

    // there should be 6 tokens
    assert.Equal(t, len(tokens), 6)

    // expecting from token and validating text
    assert.Equal(t, TokenFrom, tokens[0].Type)
    assert.Equal(t, "from", tokens[0].Value)

    // expecting package name token and validating text
    assert.Equal(t, TokenPackageName, tokens[1].Type)
    assert.Equal(t, "project.users", tokens[1].Value)

    // expecting import token and validating text
    assert.Equal(t, TokenImport, tokens[2].Type)
    assert.Equal(t, "import", tokens[2].Value)

    // expecting identifier token and validating text
    assert.Equal(t, TokenIdentifier, tokens[3].Type)
    assert.Equal(t, "User", tokens[3].Value)

    // expecting identifier token and validating text
    assert.Equal(t, TokenIdentifier, tokens[4].Type)
    assert.Equal(t, "Gadget", tokens[4].Value)

    // expecting identifier token and validating text
    assert.Equal(t, TokenIdentifier, tokens[5].Type)
    assert.Equal(t, "Widget", tokens[5].Value)
}

func TestImportAllParsing(t *testing.T) {

    text := `from project.users import *`
    // var token Token
    var tokens []Token
    l := NewLexer("tuple", text, func(t Token) {
        // fmt.Println("handler: ", t)
        // token = t
        tokens = append(tokens, t)
    })

    // lex content
    l.run()

    // there should be 6 tokens
    assert.Equal(t, len(tokens), 4)

    // expecting from token and validating text
    assert.Equal(t, TokenFrom, tokens[0].Type)
    assert.Equal(t, "from", tokens[0].Value)

    // expecting package name token and validating text
    assert.Equal(t, TokenPackageName, tokens[1].Type)
    assert.Equal(t, "project.users", tokens[1].Value)

    // expecting import token and validating text
    assert.Equal(t, TokenImport, tokens[2].Type)
    assert.Equal(t, "import", tokens[2].Value)

    // expecting asterisk token and validating text
    assert.Equal(t, TokenAsterisk, tokens[3].Type)
    assert.Equal(t, "*", tokens[3].Value)
}

func TestTypeDef(t *testing.T) {

    text := `type User {}`
    var tokens []Token
    l := NewLexer("TypeDef", text, func(t Token) {
        // fmt.Println("handler: ", t)
        // token = t
        tokens = append(tokens, t)
    })

    // lex content
    l.run()
    // t.Log(tokens)

    // there should be 4 tokens
    assert.Equal(t, len(tokens), 4)

    // expecting type token and validating text
    assert.Equal(t, TokenTypeDef, tokens[0].Type)
    assert.Equal(t, "type", tokens[0].Value)

    // expecting identifier token and validating text
    assert.Equal(t, TokenIdentifier, tokens[1].Type)
    assert.Equal(t, "User", tokens[1].Value)

    // expecting openScope token and validating text
    assert.Equal(t, TokenOpenCurlyBracket, tokens[2].Type)
    assert.Equal(t, "{", tokens[2].Value)

    // expecting closeScope token and validating text
    assert.Equal(t, TokenCloseCurlyBracket, tokens[3].Type)
    assert.Equal(t, "}", tokens[3].Value)
}

func TestIdentifier(t *testing.T) {

    text := `User.`
    var tokens []Token
    l := NewLexer("TestIdentifier", text, func(t Token) {
        tokens = append(tokens, t)
    })

    // lex content
    lexIdentifier(l, nil, false)
    // t.Log(tokens)

    // there should be 1 token
    assert.Equal(t, len(tokens), 1)

    // expecting identifier token and validating text
    assert.Equal(t, TokenIdentifier, tokens[0].Type)
    assert.Equal(t, "User", tokens[0].Value)
}

func TestVersion(t *testing.T) {

    text := `version 1`
    var tokens []Token
    l := NewLexer("TestVersion", text, func(t Token) {
        tokens = append(tokens, t)
    })

    // lex content
    lexVersion(l)
    // t.Log(tokens)

    // there should be 2 tokens
    assert.Equal(t, len(tokens), 2)

    // expecting version token and validating text
    assert.Equal(t, TokenVersion, tokens[0].Type)
    assert.Equal(t, "version", tokens[0].Value)

    // expecting version number token and validating text
    assert.Equal(t, TokenVersionNumber, tokens[1].Type)
    assert.Equal(t, "1", tokens[1].Value)
}

func TestVersionFail(t *testing.T) {

    text := `version abc`
    var tokens []Token
    l := NewLexer("TestVersionFail", text, func(t Token) {
        tokens = append(tokens, t)
    })

    // lex content
    l.run()
    // lexVersion(l)
    // t.Log(tokens)

    // there should be 4 tokens
    assert.Equal(t, len(tokens), 4)

    // expecting version token and validating text
    assert.Equal(t, TokenVersion, tokens[0].Type)
    assert.Equal(t, "version", tokens[0].Value)

    // expecting error token and validating text
    assert.Equal(t, TokenError, tokens[1].Type)
    assert.Equal(t, "TestVersionFail: unknown token: \"a\"", tokens[1].Value)

    // expecting error token and validating text
    assert.Equal(t, TokenError, tokens[2].Type)
    assert.Equal(t, "TestVersionFail: unknown token: \"b\"", tokens[2].Value)

    // expecting error token and validating text
    assert.Equal(t, TokenError, tokens[3].Type)
    assert.Equal(t, "TestVersionFail: unknown token: \"c\"", tokens[3].Value)
}

func TestOpenScope(t *testing.T) {

    text := `{`
    var tokens []Token
    l := NewLexer("TestOpenScope", text, func(t Token) {
        tokens = append(tokens, t)
    })

    // lex content
    l.run()

    // there should be 1 token
    assert.Equal(t, len(tokens), 1)

    // expecting openScope token and validating text
    assert.Equal(t, TokenOpenCurlyBracket, tokens[0].Type)
    assert.Equal(t, "{", tokens[0].Value)
}

func TestCloseScope(t *testing.T) {

    text := `}`
    var tokens []Token
    l := NewLexer("TestCloseScope", text, func(t Token) {
        tokens = append(tokens, t)
    })

    // lex content
    l.run()

    // there should be 1 token
    assert.Equal(t, len(tokens), 1)

    // expecting closeScope token and validating text
    assert.Equal(t, TokenCloseCurlyBracket, tokens[0].Type)
    assert.Equal(t, "}", tokens[0].Value)
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
