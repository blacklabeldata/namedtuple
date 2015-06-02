package namedtuple

import (
	// "fmt"
	// "github.com/stretchr/testify/assert"
	"testing"
)

func BenchmarkPutField_1(b *testing.B) {

	User := createTestTupleType()

	// create builder
	buffer := make([]byte, 1024)
	builder := NewBuilder(User, buffer)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.PutString("uuid", "0123456789abcdef")
		// builder.PutString("username", "username")
		// builder.PutUint8("age", uint8(25))
		builder.reset()
	}
}

func BenchmarkSmallTuple(b *testing.B) {

	Image := New("testing", "Image")
	Image.AddVersion(
		Field{"url", true, StringField},
		Field{"title", true, StringField},
		Field{"width", true, Uint32Field},
		Field{"height", true, Uint32Field},
		Field{"size", true, Uint8Field},
	)

	// create builder
	buffer := make([]byte, 128)
	builder := NewBuilder(Image, buffer)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.PutString("url", "a")
		builder.PutString("title", "b")
		builder.PutUint32("width", uint32(1))
		builder.PutUint32("height", uint32(2))
		builder.PutUint8("size", uint8(0))
		builder.reset()
	}
}
