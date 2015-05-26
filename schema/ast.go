package schema

// Package contains an entire schema document.
type Package struct {
    Name    string
    Imports []Import
    Types   []Type
}

// Import references one or more Types from another Package
type Import struct {
    PackageName string
    TypeNames   []string
}

// Type represents a data type. It encapsulates several versions, each with their own fields.
type Type struct {
    Name     string
    Versions []Version
}

// Version is the only construct for adding one or more Fields to a Type.
type Version struct {
    Number int
    Fields []Field
}

// Field is the lowest level of granularity in a schema. Fields belong to a single Version within a Type. They are effectively immutable and should not be changed.
type Field struct {
    IsRequired bool
    IsArray    bool
    Type       string
    Name       string
}
