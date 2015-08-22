package namedtuple

import (
	// "fmt"
	// "github.com/stretchr/testify/assert"
	"bytes"
	"testing"
	"time"
)

func BenchmarkPutField_1(b *testing.B) {
	User := createTestTupleType()

	// create builder
	buffer := make([]byte, 1024)
	builder := NewBuilder(User, buffer)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.PutString("uuid", "0123456789abcdef")
		builder.PutString("username", "username")
		builder.PutUint8("age", uint8(25))
		builder.Build()
		builder.reset()
	}
}

type A struct {
	Name     string
	BirthDay time.Time
	Phone    string
	Siblings int
	Spouse   bool
	Money    float64
}

func BenchmarkBuild(b *testing.B) {
	// Benchmark type
	AType := New("testing", "A")

	// Version 1
	AType.AddVersion(
		Field{"Name", true, StringField},
		Field{"BirthDay", true, TimestampField},
		Field{"Phone", true, StringField},
		Field{"Siblings", true, Uint8Field},
		// Field{"Spouse", true, BooleanField},
		Field{"Money", true, Float32Field},
	)

	// create builder
	buffer := make([]byte, 1024)
	builder := NewBuilder(AType, buffer)

	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.PutString("Name", "Bugs Bunny")
		builder.PutString("Phone", "555-555-5555")
		builder.PutUint8("Siblings", uint8(0))
		builder.PutTimestamp("BirthDay", now)
		// builder.PutBoolean("Spouse", false)
		builder.PutFloat32("Money", 999.99)
		builder.Build()
		builder.reset()
	}
}

func BenchmarkEncode(b *testing.B) {
	// Benchmark type
	AType := New("testing", "A")

	// Version 1
	AType.AddVersion(
		Field{"Name", true, StringField},
		Field{"BirthDay", true, TimestampField},
		Field{"Phone", true, StringField},
		Field{"Siblings", true, Uint8Field},
		Field{"Spouse", true, BooleanField},
		Field{"Money", true, Float32Field},
	)

	var buf []byte
	out := bytes.NewBuffer(buf)
	encoder := NewEncoder(out)

	// create builder
	buffer := make([]byte, 1024)
	builder := NewBuilder(AType, buffer)

	now := time.Now()
	builder.PutString("Name", "Bugs Bunny")
	builder.PutString("Phone", "555-555-5555")
	builder.PutUint8("Siblings", uint8(0))
	builder.PutTimestamp("BirthDay", now)
	// builder.PutBoolean("Spouse", false)
	builder.PutFloat32("Money", 999.99)
	a, _ := builder.Build()

	encoder.Encode(a)
	// b.SetBytes(int64(out.Len()))
	out.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(a)
		out.Reset()
	}

}

func BenchmarkDecode(b *testing.B) {
	// Benchmark type
	AType := New("testing", "A")

	// Version 1
	AType.AddVersion(
		Field{"Name", true, StringField},
		Field{"BirthDay", true, TimestampField},
		Field{"Phone", true, StringField},
		Field{"Siblings", true, Uint8Field},
		Field{"Spouse", true, BooleanField},
		Field{"Money", true, Float32Field},
	)

	// Create registry
	DefaultRegistry.Register(AType)

	var buf []byte
	out := bytes.NewBuffer(buf)
	encoder := NewEncoder(out)
	decoder := NewDecoderSize(DefaultRegistry, 1024, out)

	// create builder
	buffer := make([]byte, 1024)
	builder := NewBuilder(AType, buffer)

	now := time.Now()
	builder.PutString("Name", "Bugs Bunny")
	builder.PutString("Phone", "555-555-5555")
	builder.PutUint8("Siblings", uint8(0))
	builder.PutTimestamp("BirthDay", now)
	// builder.PutBoolean("Spouse", false)
	builder.PutFloat32("Money", 999.99)
	a, _ := builder.Build()

	encoder.Encode(a)
	// b.SetBytes(int64(out.Len()))
	out.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder.Decode()
		out.Reset()
	}
}
