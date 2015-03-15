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
		builder.PutString("username", "username")
		builder.PutUint8("age", uint8(25))
		builder.reset()
	}
}
