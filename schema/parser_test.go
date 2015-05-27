package schema

import "testing"

func TestParse(t *testing.T) {

    text := `
    package users

    from locale import Location
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

    pkgList := NewPackageList()
    config := Config{}

    // create parser
    parser := NewParser(pkgList, config)
    pkg, err := parser.Parse("TestParse", text)

    // t.Logf("%#v\n", pkg)
    // t.Log(err)
}

func BenchmarkParse(b *testing.B) {

    text := `
    package users

    from locale import Location

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

    pkgList := NewPackageList()
    config := Config{}

    // create parser
    parser := NewParser(pkgList, config)

    for i := 0; i < b.N; i++ {
        parser.Parse("TestParse", text)
    }
    // t.Logf("%#v\n", pkg)
    // t.Log(err)
}
