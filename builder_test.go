package namedtuple

import (
	// "fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

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
	assert.NotNil(t, builder.typeCheck("uuid", DateField))
}

func TestBuilderPutUint8Fail(t *testing.T) {
	// create test type
	User := createTestTupleType()

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(User, buffer)

	// fails type check
	wrote, err := builder.PutUint8("uuid", uint8(20))
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)

	// fails length check
	wrote, err = builder.PutUint8("age", uint8(20))
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)
}

func TestBuilderPutUint8(t *testing.T) {
	// create test type
	User := createTestTupleType()

	// create builder
	buffer := make([]byte, 2)
	builder := NewBuilder(User, buffer)

	// successful write
	wrote, err := builder.PutUint8("age", uint8(20))
	assert.Nil(t, err)
	assert.Equal(t, 2, wrote)
}
