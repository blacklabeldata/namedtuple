package namedtuple

import (
	// "fmt"
	"github.com/eliquious/xbinary"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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

//
func TestBuilderPutUint64Fail_1(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails type check
	wrote, err := builder.PutUint64("int64", uint64(20))
	// fmt.Println(err)
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)

	// fails length check
	wrote, err = builder.PutUint64("uint64", uint64(20))
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)
}

func TestBuilderPutUint64Pass_1(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 2)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutUint64("uint64", uint64(20))
	assert.Nil(t, err)
	assert.Equal(t, 2, wrote)

	// test data validity
	assert.Equal(t, UnsignedLong8Code.OpCode, int(builder.buffer[0]))
	assert.Equal(t, 20, int(builder.buffer[1]))

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint64"])
}

func TestBuilderPutUint64Fail_2(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutUint64("uint64", uint64(300))
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestBuilderPutUint64Pass_2(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 5)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutUint64("uint64", uint64(300))
	assert.Nil(t, err)
	assert.Equal(t, 3, wrote)

	// test data validity
	assert.Equal(t, UnsignedLong16Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Uint16(buffer, 1)
	assert.Equal(t, uint16(300), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint64"])
}

func TestBuilderPutUint64Fail_3(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 3)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutUint64("uint64", uint64(135000))
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestBuilderPutUint64Pass_3(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 5)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutUint64("uint64", uint64(135000))
	assert.Nil(t, err)
	assert.Equal(t, 5, wrote)

	// test data validity
	assert.Equal(t, UnsignedLong32Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Uint32(buffer, 1)
	assert.Equal(t, uint64(135000), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint64"])
}

func TestBuilderPutUint64Fail_4(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 5)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutUint64("uint64", uint64(17179869184)) // 2^34
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestBuilderPutUint64Pass_4(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 9)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutUint64("uint64", uint64(17179869184)) // 2^34
	assert.Nil(t, err)
	assert.Equal(t, 9, wrote)

	// test data validity
	assert.Equal(t, UnsignedLong64Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Uint64(buffer, 1)
	assert.Equal(t, uint64(17179869184), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint64"])
}

//
func TestBuilderPutInt64Fail_1(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails type check
	wrote, err := builder.PutInt64("uint64", int64(20))
	// fmt.Println(err)
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)

	// fails length check
	wrote, err = builder.PutInt64("int64", int64(20))
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)
}

func TestBuilderPutInt64Pass_1(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 2)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutInt64("int64", int64(20))
	assert.Nil(t, err)
	assert.Equal(t, 2, wrote)

	// test data validity
	assert.Equal(t, Long8Code.OpCode, int(builder.buffer[0]))
	assert.Equal(t, 20, int(builder.buffer[1]))

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint64"])
}

func TestBuilderPutInt64Fail_2(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutInt64("int64", int64(300))
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestBuilderPutInt64Pass_2(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 5)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutInt64("int64", int64(300))
	assert.Nil(t, err)
	assert.Equal(t, 3, wrote)

	// test data validity
	assert.Equal(t, Long16Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Int16(buffer, 1)
	assert.Equal(t, int16(300), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint64"])
}

func TestBuilderPutInt64Fail_3(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 3)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutInt64("int64", int64(135000))
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestBuilderPutInt64Pass_3(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 5)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutInt64("int64", int64(135000))
	assert.Nil(t, err)
	assert.Equal(t, 5, wrote)

	// test data validity
	assert.Equal(t, Long32Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Int32(buffer, 1)
	assert.Equal(t, int64(135000), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint64"])
}

func TestBuilderPutInt64Fail_4(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 5)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutInt64("int64", int64(17179869184)) // 2^34
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestBuilderPutInt64Pass_4(t *testing.T) {

	// create test type
	// integer test type
	TestType := New("uint64")
	TestType.AddVersion(
		Field{"int64", true, Int64Field},
		Field{"uint64", true, Uint64Field},
	)

	// create builder
	buffer := make([]byte, 9)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutInt64("int64", int64(17179869184)) // 2^34
	assert.Nil(t, err)
	assert.Equal(t, 9, wrote)

	// test data validity
	assert.Equal(t, Long64Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Int64(buffer, 1)
	assert.Equal(t, int64(17179869184), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["uint64"])
}

// Float32
func TestPutFloat32Fail(t *testing.T) {

	// create test type
	// float test type
	TestType := New("float")
	TestType.AddVersion(
		Field{"float32", true, Float32Field},
		Field{"float64", true, Float64Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails type check
	wrote, err := builder.PutFloat32("float64", float32(3.14159))
	// fmt.Println(err)
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)

	// fails length check
	wrote, err = builder.PutFloat32("float32", float32(3.14159))
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)
}

func TestPutFloat32Pass(t *testing.T) {

	// create test type
	// float test type
	TestType := New("float")
	TestType.AddVersion(
		Field{"float32", true, Float32Field},
		Field{"float64", true, Float64Field},
	)

	// create builder
	buffer := make([]byte, 5)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutFloat32("float32", float32(3.14159))
	assert.Nil(t, err)
	assert.Equal(t, 5, wrote)

	// test data validity
	assert.Equal(t, FloatCode.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Float32(buffer, 1)
	assert.Equal(t, float32(3.14159), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["float32"])
}

// Float64
func TestPutFloat64Fail(t *testing.T) {

	// create test type
	// float test type
	TestType := New("float")
	TestType.AddVersion(
		Field{"float32", true, Float32Field},
		Field{"float64", true, Float64Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails type check
	wrote, err := builder.PutFloat64("float32", float64(3.14159))
	// fmt.Println(err)
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)

	// fails length check
	wrote, err = builder.PutFloat64("float64", float64(3.14159))
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestPutFloat64Pass(t *testing.T) {

	// create test type
	// float test type
	TestType := New("float")
	TestType.AddVersion(
		Field{"float32", true, Float32Field},
		Field{"float64", true, Float64Field},
	)

	// create builder
	buffer := make([]byte, 9)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutFloat64("float64", float64(3.14159))
	assert.Nil(t, err)
	assert.Equal(t, 9, wrote)

	// test data validity
	assert.Equal(t, DoubleCode.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Float64(buffer, 1)
	assert.Equal(t, float64(3.14159), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["float64"])
}

// time testing
func TestPutTimestampFail(t *testing.T) {

	// create test type
	// float test type
	TestType := New("time")
	TestType.AddVersion(
		Field{"timestamp", true, TimestampField},
		Field{"float64", true, Float64Field},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails type check
	wrote, err := builder.PutTimestamp("float64", time.Now())
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)

	// fails length check
	wrote, err = builder.PutTimestamp("timestamp", time.Now())
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestPutTimestampPass(t *testing.T) {

	// create test type
	// float test type
	TestType := New("time")
	TestType.AddVersion(
		Field{"timestamp", true, TimestampField},
		Field{"float64", true, Float64Field},
	)

	// create builder
	buffer := make([]byte, 9)
	builder := NewBuilder(TestType, buffer)

	// successful write
	now := time.Now()
	wrote, err := builder.PutTimestamp("timestamp", now)
	assert.Nil(t, err)
	assert.Equal(t, 9, wrote)

	// test data validity
	assert.Equal(t, TimestampCode.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Int64(buffer, 1)
	assert.Equal(t, now.UnixNano(), value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["timestamp"])
}

// String
func TestPutStringFail_1(t *testing.T) {
	// create test type
	// float test type
	TestType := New("string")
	TestType.AddVersion(
		Field{"string", true, StringField},
		Field{"bool", true, BooleanField},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails type check
	wrote, err := builder.PutString("bool", "namedtuple")
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)

	// fails length check
	wrote, err = builder.PutString("string", "namedtuple")
	assert.NotNil(t, err)
	assert.Equal(t, 0, wrote)
}

func TestPutStringPass_1(t *testing.T) {
	// create test type
	// float test type
	TestType := New("string")
	TestType.AddVersion(
		Field{"string", true, StringField},
		Field{"bool", true, BooleanField},
	)

	// create builder
	buffer := make([]byte, 12)
	builder := NewBuilder(TestType, buffer)

	// successful write
	wrote, err := builder.PutString("string", "namedtuple")
	assert.Nil(t, err)
	assert.Equal(t, 12, wrote)

	// test data validity
	assert.Equal(t, String8Code.OpCode, int(builder.buffer[0]))
	assert.Equal(t, 10, int(builder.buffer[1]))

	value, err := xbinary.LittleEndian.String(buffer, 2, 10)
	assert.Equal(t, "namedtuple", value)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["string"])
}
func TestPutStringFail_2(t *testing.T) {
	// create test type
	// float test type
	TestType := New("string")
	TestType.AddVersion(
		Field{"string", true, StringField},
		Field{"bool", true, BooleanField},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutString("string", string(make([]byte, 300)))
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestPutStringPass_2(t *testing.T) {
	// create test type
	// float test type
	TestType := New("string")
	TestType.AddVersion(
		Field{"string", true, StringField},
		Field{"bool", true, BooleanField},
	)

	// create builder
	buffer := make([]byte, 303)
	builder := NewBuilder(TestType, buffer)

	// successful write
	input := string(make([]byte, 300))
	wrote, err := builder.PutString("string", input)
	assert.Nil(t, err)
	assert.Equal(t, 303, wrote)

	// test data validity
	assert.Equal(t, String16Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Uint16(buffer, 1)
	assert.Equal(t, 300, int(value))

	output, err := xbinary.LittleEndian.String(buffer, 3, int(value))
	assert.Equal(t, input, output)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["string"])
}
func TestPutStringFail_3(t *testing.T) {
	// create test type
	// float test type
	TestType := New("string")
	TestType.AddVersion(
		Field{"string", true, StringField},
		Field{"bool", true, BooleanField},
	)

	// create builder
	buffer := make([]byte, 1)
	builder := NewBuilder(TestType, buffer)

	// fails length check
	wrote, err := builder.PutString("string", string(make([]byte, 135000)))
	assert.NotNil(t, err)
	assert.Equal(t, 1, wrote)
}

func TestPutStringPass_3(t *testing.T) {
	// create test type
	// float test type
	TestType := New("string")
	TestType.AddVersion(
		Field{"string", true, StringField},
		Field{"bool", true, BooleanField},
	)

	// create builder
	buffer := make([]byte, 135005)
	builder := NewBuilder(TestType, buffer)

	// successful write
	input := string(make([]byte, 135000))
	wrote, err := builder.PutString("string", input)
	assert.Nil(t, err)
	assert.Equal(t, 135005, wrote)

	// test data validity
	assert.Equal(t, String32Code.OpCode, int(builder.buffer[0]))

	value, err := xbinary.LittleEndian.Uint32(buffer, 1)
	assert.Equal(t, 135000, int(value))

	output, err := xbinary.LittleEndian.String(buffer, 5, int(value))
	assert.Equal(t, input, output)

	// validate field offset
	assert.Equal(t, 0, builder.offsets["string"])
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
