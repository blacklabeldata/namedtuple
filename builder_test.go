package namedtuple

import (
	// "fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// func createUintTestType() TupleType {

// 	// unsigned integer test type
// 	UintTestType := New("uint")

// 	// Integers
// 	UintTestType.AddVersion(
// 		Field{"uint8-8", true, Uint8Field},
// 		Field{"uint16-8", true, Uint16Field},
// 		Field{"uint16-16", true, Uint16Field},
// 		Field{"uint32-8", true, Uint32Field},
// 		Field{"uint32-16", true, Uint32Field},
// 		Field{"uint32-32", true, Uint32Field},
// 		Field{"uint64-8", true, Uint64Field},
// 		Field{"uint64-16", true, Uint64Field},
// 		Field{"uint64-32", true, Uint64Field},
// 		Field{"uint64-64", true, Uint64Field},
// 	)
// 	// Arrays
// 	UintTestType.AddVersion(
// 		Field{"uint8-8-array", true, Uint8FieldArray},
// 		Field{"uint16-8-array", true, Uint16FieldArray},
// 		Field{"uint16-1-array6", true, Uint16FieldArray},
// 		Field{"uint32-8-array", true, Uint32FieldArray},
// 		Field{"uint32-1-array6", true, Uint32FieldArray},
// 		Field{"uint32-3-array2", true, Uint32FieldArray},
// 		Field{"uint64-8-array", true, Uint64FieldArray},
// 		Field{"uint64-1-array6", true, Uint64FieldArray},
// 		Field{"uint64-3-array2", true, Uint64FieldArray},
// 		Field{"uint64-6-array4", true, Uint64FieldArray},
// 	)
// 	return UintTestType
// }

func TestNewBuilder(t *testing.T) {

	// create test type
	User := createTestTupleType()

	// create builder
	buffer := make([]byte, 1024)
	builder := NewBuilder(User, buffer)

	// verify type
	assert.Equal(t, User, builder.tupleType)

	// verify type fields
	assert.Equal(t, len(User.fields), len(builder.fields))
	for name, _ := range builder.fields {

		// make sure the type has the same fields as the builder
		assert.True(t, User.Contains(name))
	}
}

func TestBuilderAvailableEmpty(t *testing.T) {
	// create test type
	User := createTestTupleType()

	// create builder
	buffer := make([]byte, 1024)
	builder := NewBuilder(User, buffer)

	// verify available == 1024
	assert.Equal(t, 1024, builder.available())

}

func TestBuilderTypeCheck(t *testing.T) {
	// create test type
	User := createTestTupleType()

	// create builder
	buffer := make([]byte, 1024)
	builder := NewBuilder(User, buffer)

	// testing correct fields
	assert.Nil(t, builder.typeCheck("uuid", StringField))
	assert.Nil(t, builder.typeCheck("username", StringField))
	assert.Nil(t, builder.typeCheck("age", Uint8Field))
	assert.Nil(t, builder.typeCheck("location", TupleField))

	// testing invalid field
	assert.NotNil(t, builder.typeCheck("school", StringField))

	// testing invalid type
	assert.NotNil(t, builder.typeCheck("uuid", TimestampField))
}

// building
func TestBuild(t *testing.T) {

	// type
	User := createTestTupleType()

	// create builder
	buffer := make([]byte, 1024)
	builder := NewBuilder(User, buffer)

	// fields
	builder.PutString("username", "value")
	builder.PutString("uuid", "value")
	builder.PutUint8("age", 25)

	// tuple
	// user := builder.Build()

}
