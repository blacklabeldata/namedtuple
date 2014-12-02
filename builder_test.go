package namedtuple

import (
	// "fmt"
	"github.com/eliquious/xbinary"
	"github.com/stretchr/testify/assert"
	"testing"
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

func TestBuilderPutUint8Pass(t *testing.T) {
	// create test type
	User := createTestTupleType()

	// create builder
	buffer := make([]byte, 2)
	builder := NewBuilder(User, buffer)

	// successful write
	wrote, err := builder.PutUint8("age", uint8(20))
	assert.Nil(t, err)
	assert.Equal(t, 2, wrote)

	// test data validity
	assert.Equal(t, UnsignedInt8Code.OpCode, int(builder.buffer[0]))
	assert.Equal(t, 20, int(builder.buffer[1]))

	// validate field offset
	assert.Equal(t, 0, builder.offsets["age"])
}

func TestBuilderPutInt8Fail(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("int8")
	TestType.AddVersion(
		Field{"int8", true, Int8Field},
		Field{"uint8", true, Uint8Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails type check
	wrote, err := builder.PutInt8("uint8", int8(20))
	// fmt.Println(err)
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)

	// fails length check
	wrote, err = builder.PutInt8("int8", int8(20))
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)
}

func TestBuilderPutInt8Pass(t *testing.T) {
	// create test type
	// integer test type
	TestType := New("int8")
	TestType.AddVersion(
		Field{"int8", true, Int8Field},
		Field{"uint8", true, Uint8Field},
	)

	// create builder
	buffer := make([]byte, 2)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutInt8("int8", int8(20))
	assert.Nil(t, err)
	assert.Equal(t, 2, wrote)

	// test data validity
	assert.Equal(t, Int8Code.OpCode, int(builder.buffer[0]))
	assert.Equal(t, 20, int(builder.buffer[1]))

	// validate field offset
	assert.Equal(t, 0, builder.offsets["int8"])
}

func TestBuilderPutUint16Fail_1(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint16")
	TestType.AddVersion(
		Field{"int16", true, Int16Field},
		Field{"uint16", true, Uint16Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails type check
	wrote, err := builder.PutUint16("int16", uint16(20))
	// fmt.Println(err)
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)

	// fails length check
	wrote, err = builder.PutUint16("uint16", uint16(20))
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)
}

func TestBuilderPutUint16Pass_1(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint16")
	TestType.AddVersion(
		Field{"int16", true, Int16Field},
		Field{"uint16", true, Uint16Field},
	)

	// create builder
	buffer := make([]byte, 2)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutUint16("uint16", uint16(20))
	assert.Nil(t, err)
	assert.Equal(t, 2, wrote)

	// test data validity
	assert.Equal(t, UnsignedShort8Code.OpCode, int(builder.buffer[0]))
	assert.Equal(t, 20, int(builder.buffer[1]))

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint16"])
}

func TestBuilderPutUint16Fail_2(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint16")
	TestType.AddVersion(
		Field{"int16", true, Int16Field},
		Field{"uint16", true, Uint16Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutUint16("uint16", uint16(300))
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestBuilderPutUint16Pass_2(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint16")
	TestType.AddVersion(
		Field{"int16", true, Int16Field},
		Field{"uint16", true, Uint16Field},
	)

	// create builder
	buffer := make([]byte, 3)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutUint16("uint16", uint16(300))
	assert.Nil(t, err)
	assert.Equal(t, 3, wrote)

	// test data validity
	assert.Equal(t, UnsignedShort16Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Uint16(buffer, 1)
	assert.Equal(t, uint16(300), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint16"])
}

//
func TestBuilderPutInt16Fail_1(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("int16")
	TestType.AddVersion(
		Field{"int16", true, Int16Field},
		Field{"uint16", true, Uint16Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails type check
	wrote, err := builder.PutInt16("uint16", int16(20))
	// fmt.Println(err)
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)

	// fails length check
	wrote, err = builder.PutInt16("int16", int16(20))
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)
}

func TestBuilderPutInt16Pass_1(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("int16")
	TestType.AddVersion(
		Field{"int16", true, Int16Field},
		Field{"uint16", true, Uint16Field},
	)

	// create builder
	buffer := make([]byte, 2)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutInt16("int16", int16(20))
	assert.Nil(t, err)
	assert.Equal(t, 2, wrote)

	// test data validity
	assert.Equal(t, Short8Code.OpCode, int(builder.buffer[0]))
	assert.Equal(t, 20, int(builder.buffer[1]))

	// validate field offset
	assert.Equal(t, 0, builder.offsets["int16"])
}

func TestBuilderPutInt16Fail_2(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("int16")
	TestType.AddVersion(
		Field{"int16", true, Int16Field},
		Field{"uint16", true, Uint16Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutInt16("int16", int16(300))
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestBuilderPutInt16Pass_2(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("int16")
	TestType.AddVersion(
		Field{"int16", true, Int16Field},
		Field{"uint16", true, Uint16Field},
	)

	// create builder
	buffer := make([]byte, 3)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutInt16("int16", int16(-300))
	assert.Nil(t, err)
	assert.Equal(t, 3, wrote)

	// test data validity
	assert.Equal(t, Short16Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Int16(buffer, 1)
	assert.Equal(t, int16(-300), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["int16"])
}

//
func TestBuilderPutUint32Fail_1(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint32")
	TestType.AddVersion(
		Field{"int32", true, Int32Field},
		Field{"uint32", true, Uint32Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails type check
	wrote, err := builder.PutUint32("int32", uint32(20))
	// fmt.Println(err)
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)

	// fails length check
	wrote, err = builder.PutUint32("uint32", uint32(20))
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)
}

func TestBuilderPutUint32Pass_1(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint32")
	TestType.AddVersion(
		Field{"int32", true, Int32Field},
		Field{"uint32", true, Uint32Field},
	)

	// create builder
	buffer := make([]byte, 2)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutUint32("uint32", uint32(20))
	assert.Nil(t, err)
	assert.Equal(t, 2, wrote)

	// test data validity
	assert.Equal(t, UnsignedInt8Code.OpCode, int(builder.buffer[0]))
	assert.Equal(t, 20, int(builder.buffer[1]))

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint32"])
}

func TestBuilderPutUint32Fail_2(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint32")
	TestType.AddVersion(
		Field{"int32", true, Int32Field},
		Field{"uint32", true, Uint32Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutUint32("uint32", uint32(300))
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestBuilderPutUint32Pass_2(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint32")
	TestType.AddVersion(
		Field{"int32", true, Int32Field},
		Field{"uint32", true, Uint32Field},
	)

	// create builder
	buffer := make([]byte, 5)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutUint32("uint32", uint32(300))
	assert.Nil(t, err)
	assert.Equal(t, 3, wrote)

	// test data validity
	assert.Equal(t, UnsignedInt16Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Uint32(buffer, 1)
	assert.Equal(t, uint16(300), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint32"])
}

func TestBuilderPutUint32Fail_3(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint32")
	TestType.AddVersion(
		Field{"int32", true, Int32Field},
		Field{"uint32", true, Uint32Field},
	)

	// create builder
	buffer := make([]byte, 3)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutUint32("uint32", uint32(135000))
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestBuilderPutUint32Pass_3(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint32")
	TestType.AddVersion(
		Field{"int32", true, Int32Field},
		Field{"uint32", true, Uint32Field},
	)

	// create builder
	buffer := make([]byte, 5)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutUint32("uint32", uint32(135000))
	assert.Nil(t, err)
	assert.Equal(t, 5, wrote)

	// test data validity
	assert.Equal(t, UnsignedInt32Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Uint32(buffer, 1)
	assert.Equal(t, uint32(135000), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint32"])
}

//
func TestBuilderPutInt32Fail_1(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint32")
	TestType.AddVersion(
		Field{"int32", true, Int32Field},
		Field{"uint32", true, Uint32Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails type check
	wrote, err := builder.PutInt32("uint32", int32(20))
	// fmt.Println(err)
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)

	// fails length check
	wrote, err = builder.PutInt32("int32", int32(20))
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)
}

func TestBuilderPutInt32Pass_1(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint32")
	TestType.AddVersion(
		Field{"int32", true, Int32Field},
		Field{"uint32", true, Uint32Field},
	)

	// create builder
	buffer := make([]byte, 2)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutInt32("int32", int32(20))
	assert.Nil(t, err)
	assert.Equal(t, 2, wrote)

	// test data validity
	assert.Equal(t, Int8Code.OpCode, int(builder.buffer[0]))
	assert.Equal(t, 20, int(builder.buffer[1]))

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint32"])
}

func TestBuilderPutInt32Fail_2(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint32")
	TestType.AddVersion(
		Field{"int32", true, Int32Field},
		Field{"uint32", true, Uint32Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutInt32("int32", int32(300))
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestBuilderPutInt32Pass_2(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint32")
	TestType.AddVersion(
		Field{"int32", true, Int32Field},
		Field{"uint32", true, Uint32Field},
	)

	// create builder
	buffer := make([]byte, 5)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutInt32("int32", int32(300))
	assert.Nil(t, err)
	assert.Equal(t, 3, wrote)

	// test data validity
	assert.Equal(t, Int16Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Int32(buffer, 1)
	assert.Equal(t, uint16(300), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint32"])
}

func TestBuilderPutInt32Fail_3(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint32")
	TestType.AddVersion(
		Field{"int32", true, Int32Field},
		Field{"uint32", true, Uint32Field},
	)

	// create builder
	buffer := make([]byte, 3)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutInt32("int32", int32(135000))
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestBuilderPutInt32Pass_3(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint32")
	TestType.AddVersion(
		Field{"int32", true, Int32Field},
		Field{"uint32", true, Uint32Field},
	)

	// create builder
	buffer := make([]byte, 5)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutInt32("int32", int32(135000))
	assert.Nil(t, err)
	assert.Equal(t, 5, wrote)

	// test data validity
	assert.Equal(t, Int32Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Int32(buffer, 1)
	assert.Equal(t, uint32(135000), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint32"])
}
