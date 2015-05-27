package schema

import (
    "strconv"
    "sync"
)

// Config simply stores the parsing configuration.
type Config struct {
    PackageRootDir string
}

// SyntaxError represents an error while parsing the schema
type SyntaxError struct {
    Message string
}

func (s SyntaxError) Error() string {
    return s.Message
}

// // LoadFile reads a schema document from a file.
// func LoadFile(file *os.File, config Config) (Package, error) {

//     // read file
//     bytes, err := ioutil.ReadAll(file)
//     if err != nil {
//         return Package{}, err
//     }

//     // convert to string and load
//     return LoadPackage(string(bytes), config)
// }

// // LoadPackage parses a text string.
// func LoadPackage(text string, config Config) (Package, error) {
//     return Package{}, nil
// }

func NewParser(pkgList PackageList, config Config) Parser {
    var lock sync.Mutex
    return Parser{config, &pkgList, []Token{}, 0, lock}
}

type Parser struct {
    config  Config
    pkgList *PackageList
    tokens  []Token
    pos     int
    lock    sync.Mutex
}

func (p *Parser) Parse(name string, text string) (pkg Package, err error) {
    p.lock.Lock()
    defer p.lock.Unlock()

    l := NewLexer(name, text, func(tok Token) {
        p.tokens = append(p.tokens, tok)
    })
    l.run()

    return p.parsePackage()
}

func (p *Parser) advance(skip int) {
    p.pos += skip
}

func (p *Parser) current() (tok Token) {
    if p.pos >= len(p.tokens) {
        tok = Token{TokenError, "end of input"}
    } else {
        tok = p.tokens[p.pos]
        if tok.Type == TokenComment {
            p.advance(1)
            return p.current()
        }
    }
    return
}

func (p *Parser) next() (tok Token) {
    tok = p.current()
    p.advance(1)

    // ignore comments
    if tok.Type == TokenComment {
        tok = p.next()
    }

    return
}

func (p *Parser) backup() {
    if p.pos > 0 {
        p.pos--
    }
}

func (p *Parser) typeCheck(t TokenType, errMsg string) (tok Token, err error) {

    // next token
    tok = p.next()

    // is it an error
    if tok.Type == TokenError {
        return tok, SyntaxError{tok.Value}
    }

    // is it the correct type
    if tok.Type != t {
        return tok, SyntaxError{errMsg}
    }

    // currect token type
    return
}

func (p *Parser) parsePackage() (pkg Package, err error) {
    if len(p.tokens) == 0 {
        return pkg, SyntaxError{"empty input string"}
    }

    // consume package decl
    _, err = p.typeCheck(TokenPackage, "expected package declaration")
    if err != nil {
        return
    }

    // consume package name
    tok, err := p.typeCheck(TokenPackageName, "expected package name")
    if err != nil {
        return
    }
    pkg.Name = tok.Value

    // parse imports
    if err = p.parseImports(&pkg); err != nil {
        return
    }

    // parse types
    if err = p.parseTypes(&pkg); err != nil {
        return
    }

    return
}

func (p *Parser) parseImports(pkg *Package) (err error) {

    for p.current().Type == TokenFrom {

        var imp Import

        // consume 'from' keyword
        if _, err := p.typeCheck(TokenFrom, "expected 'from' keyword"); err != nil {
            return err
        }

        // consume package name
        tok, err := p.typeCheck(TokenPackageName, "expected package name")
        if err != nil {
            return err
        }

        // set import package name
        imp.PackageName = tok.Value

        // consume 'import' keyword
        if _, err := p.typeCheck(TokenImport, "expected 'import' keyword"); err != nil {
            return err
        }

        // consume type name
        tok, err = p.typeCheck(TokenIdentifier, "expected type name")
        if err != nil {
            return err
        }
        imp.TypeNames = append(imp.TypeNames, tok.Value)

        // consume multiple type names
        for p.current().Type == TokenComma {
            // skip comma token
            p.advance(1)

            // consume type name
            tok, err = p.typeCheck(TokenIdentifier, "expected type name")
            if err != nil {
                return err
            }
            imp.TypeNames = append(imp.TypeNames, tok.Value)
        }

        // add import to package
        pkg.Imports = append(pkg.Imports, imp)
    }

    return nil
}

func (p *Parser) parseTypes(pkg *Package) (err error) {

    // iterate over type defs
    for p.current().Type == TokenTypeDef {

        var t Type

        // consume 'type' keyword
        if _, err := p.typeCheck(TokenTypeDef, "expected 'type' keyword"); err != nil {
            return err
        }

        // consume type name
        tok, err := p.typeCheck(TokenIdentifier, "expected type name")
        if err != nil {
            return err
        }

        // set type name
        t.Name = tok.Value

        // consume open scope
        if _, err := p.typeCheck(TokenOpenCurlyBracket, "expected open bracket"); err != nil {
            return err
        }

        // parse versions
        if err = p.parseVersions(pkg, &t); err != nil {
            return err
        }

        // consume close scope
        _, err = p.typeCheck(TokenCloseCurlyBracket, "expected close bracket")
        if err != nil {
            return err
        }

        pkg.Types = append(pkg.Types, t)
    }

    return nil
}

func (p *Parser) parseVersions(pkg *Package, t *Type) (err error) {

    // iterate over versions
    for p.current().Type == TokenVersion {

        var ver Version

        // consume 'version' keyword
        if _, err := p.typeCheck(TokenVersion, "expected 'version' keyword"); err != nil {
            return err
        }

        // consume version number
        tok, err := p.typeCheck(TokenVersionNumber, "expected version number")
        if err != nil {
            return err
        }

        num, err := strconv.Atoi(tok.Value)
        if err != nil {
            return err
        }

        // set version num
        ver.Number = num

        // consume open scope
        if _, err := p.typeCheck(TokenOpenCurlyBracket, "expected open bracket"); err != nil {
            return err
        }

        // parse fields
    OUTER:
        for {
            switch p.current().Type {
            case TokenRequired, TokenOptional:
                if err = p.parseField(pkg, &ver); err != nil {
                    return err
                }
            default:
                break OUTER
            }
        }

        // consume close scope
        _, err = p.typeCheck(TokenCloseCurlyBracket, "expected close bracket")
        if err != nil {
            return err
        }

        t.Versions = append(t.Versions, ver)
    }
    return nil
}

func (p *Parser) parseField(pkg *Package, ver *Version) (err error) {

    var field Field
    switch p.current().Type {
    case TokenRequired:
        field.IsRequired = true
    case TokenOptional:
        field.IsRequired = false
    default:
        return SyntaxError{"expected 'required' or 'optional' keyword"}
    }
    p.advance(1)

    // consume optional array bracket
    tok := p.next()
    if tok.Type == TokenOpenArrayBracket {
        field.IsArray = true

        _, err = p.typeCheck(TokenCloseArrayBracket, "expected array close bracket")
        if err != nil {
            return err
        }
    } else if tok.Type == TokenError {
        return SyntaxError{tok.Value}
    } else {
        p.backup()
    }

    // field type should be next
    if p.current().Type != TokenValueType {
        return SyntaxError{"expected field type, not '" + p.current().Value + "'"}
    }

    typeName := p.current().Value
    var found bool
    for _, t := range TypeNames {
        if typeName == t {
            field.Type = typeName
            found = true
            break
        }
    }

    // eval import stmts
    if !found {
    OUTER:
        for _, imp := range pkg.Imports {
            for _, typ := range imp.TypeNames {
                if typ == typeName {
                    found = true
                    field.Type = typeName
                    break OUTER
                }
            }
        }
    }

    // if still not found
    if !found {
        return SyntaxError{"unknown type '" + typeName + "'"}
    }

    // consume field name
    p.advance(1)
    tok, err = p.typeCheck(TokenIdentifier, "expected field name")
    if err != nil {
        return err
    }
    field.Name = tok.Value

    // fmt.Printf("%#v\n", field)
    ver.Fields = append(ver.Fields, field)
    return nil
}
