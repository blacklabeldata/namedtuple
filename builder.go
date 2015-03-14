package namedtuple

import (
	"errors"
	"math"
	"time"

	"github.com/eliquious/xbinary"
)

func NewBuilder(t TupleType, buffer []byte) TupleBuilder {

	// init instance variables
	fields := make(map[string]Field)
	offsets := make(map[string]int)

	// populate instance fields for builder
	for _, version := range t.Versions() {
		for _, field := range version.Fields {
			fields[field.Name] = field
			offsets[field.Name] = 0
		}
	}

	// create new builder
	return TupleBuilder{fields: fields, offsets: offsets, tupleType: t, buffer: buffer, pos: 0}
}

type TupleBuilder struct {
	fields    map[string]Field
	offsets   map[string]int
	tupleType TupleType
	buffer    []byte
	pos       int
}

func (b TupleBuilder) available() int {
	return len(b.buffer) - b.pos
}

func (b TupleBuilder) reset() {
	b.pos = 0
}

func (t *TupleBuilder) typeCheck(fieldName string, fieldType FieldType) error {
	field, exists := t.fields[fieldName]
	if !exists {
		return errors.New("Field does not exist: " + fieldName)
	}

	if field.Type != fieldType {
		return errors.New("Incorrect field type: " + fieldName)
	}
	return nil
}

func (b *TupleBuilder) PutFloat32(field string, value float32) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, Float32Field); err != nil {
		return 0, err
	}

	// write value
	// length check performed by xbinary
	wrote, err = xbinary.LittleEndian.PutFloat32(b.buffer, b.pos+1, value)
	if err != nil {
		return 0, err
	}

	// write type code
	b.buffer[b.pos] = byte(FloatCode.OpCode)

	// set field offset
	b.offsets[field] = b.pos

	// incr pos
	b.pos += 5

	return 5, nil
}

func (b *TupleBuilder) PutFloat64(field string, value float64) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, Float64Field); err != nil {
		return 0, err
	}

	// write value
	// length check performed by xbinary
	wrote, err = xbinary.LittleEndian.PutFloat64(b.buffer, b.pos+1, value)
	if err != nil {
		return 1, err
	}

	// write type code
	b.buffer[b.pos] = byte(DoubleCode.OpCode)

	// set field offset
	b.offsets[field] = b.pos

	// incr pos
	b.pos += 9

	return 9, nil
}

func (b *TupleBuilder) PutTimestamp(field string, value time.Time) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, TimestampField); err != nil {
		return 0, err
	}

	// write value
	// length check performed by xbinary
	wrote, err = xbinary.LittleEndian.PutInt64(b.buffer, b.pos+1, value.UnixNano())
	if err != nil {
		return 1, err
	}

	// write type code
	b.buffer[b.pos] = byte(TimestampCode.OpCode)

	// set field offset
	b.offsets[field] = b.pos

	// incr pos
	b.pos += 9

	// wrote 9 bytes
	return 9, nil
}

func (b *TupleBuilder) PutTuple(field string, value Tuple) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, TupleField); err != nil {
		return 0, err
	}

	size := value.Size()
	if size < math.MaxUint8 {

		if b.available() < value.Size()+2 {
			wrote, err = value.Write(b.buffer[b.pos+2:])

			// write type code
			b.buffer[b.pos] = byte(Tuple8Code.OpCode)

			// write length
			b.buffer[b.pos+1] = byte(size)

			wrote += 2
		} else {
			return 2, xbinary.ErrOutOfRange
		}
	} else if size < math.MaxUint16 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size)); err != nil {
			return 1, err
		}

		// write type code
		b.buffer[b.pos] = byte(Tuple16Code.OpCode)

		if b.available() < value.Size()+3 {
			wrote, err = value.Write(b.buffer[b.pos+2:])
			wrote += 3
		} else {
			return 3, xbinary.ErrOutOfRange
		}
	} else if size < math.MaxUint32 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size)); err != nil {
			return 1, err
		}

		// write type code
		b.buffer[b.pos] = byte(Tuple32Code.OpCode)

		if b.available() < value.Size()+5 {
			wrote, err = value.Write(b.buffer[b.pos+5:])
			wrote += 5
		} else {
			return 5, xbinary.ErrOutOfRange
		}
	} else {

		// write length
		if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size)); err != nil {
			return 1, err
		}

		// write type code
		b.buffer[b.pos] = byte(Tuple64Code.OpCode)

		if b.available() < value.Size()+9 {
			wrote, err = value.Write(b.buffer[b.pos+9:])
			wrote += 9
		} else {
			return 9, xbinary.ErrOutOfRange
		}
	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

func (b *TupleBuilder) PutString(field string, value string) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, StringField); err != nil {
		return 0, err
	}

	size := len(value)
	if size < math.MaxUint8 {

		// write length
		if _, err = xbinary.LittleEndian.PutString(b.buffer, b.pos+2, value); err != nil {
			return 0, err
		}

		// write type code
		b.buffer[b.pos] = byte(String8Code.OpCode)

		// write length
		b.buffer[b.pos+1] = byte(size)

		wrote += size + 2
	} else if size < math.MaxUint16 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutString(b.buffer, b.pos+3, value); err != nil {
			return 3, err
		}
		// write type code
		b.buffer[b.pos] = byte(String16Code.OpCode)

		wrote += 3 + size
	} else if size < math.MaxUint32 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutString(b.buffer, b.pos+5, value); err != nil {
			return 5, err
		}
		// write type code
		b.buffer[b.pos] = byte(String32Code.OpCode)

		wrote += 5 + size
	} else {

		// write length
		if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutString(b.buffer, b.pos+9, value); err != nil {
			return 9, err
		}
		// write type code
		b.buffer[b.pos] = byte(String64Code.OpCode)

		wrote += 9 + size
	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

func (b *TupleBuilder) PutUint8Array(field string, value []uint8) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, Uint8ArrayField); err != nil {
		return 0, err
	}

	size := len(value)
	if size < math.MaxUint8 {

		// write data
		if _, err = xbinary.LittleEndian.PutUint8Array(b.buffer, b.pos+2, value); err != nil {
			return 2, err
		}

		// write type code
		b.buffer[b.pos] = byte(UnsignedByteArray8Code.OpCode)

		// write length
		b.buffer[b.pos+1] = byte(size)

		wrote += size + 2
	} else if size < math.MaxUint16 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutUint8Array(b.buffer, b.pos+3, value); err != nil {
			return 3, err
		}
		// write type code
		b.buffer[b.pos] = byte(UnsignedByteArray16Code.OpCode)

		wrote += 3 + size
	} else if size < math.MaxUint32 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutUint8Array(b.buffer, b.pos+5, value); err != nil {
			return 5, err
		}
		// write type code
		b.buffer[b.pos] = byte(UnsignedByteArray32Code.OpCode)

		wrote += 5 + size
	} else {

		// write length
		if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutUint8Array(b.buffer, b.pos+9, value); err != nil {
			return 9, err
		}
		// write type code
		b.buffer[b.pos] = byte(UnsignedByteArray64Code.OpCode)

		wrote += 9 + size
	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

func (b *TupleBuilder) PutInt8Array(field string, value []int8) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, Int8ArrayField); err != nil {
		return 0, err
	}

	size := len(value)
	if size < math.MaxUint8 {

		// write data
		if _, err = xbinary.LittleEndian.PutInt8Array(b.buffer, b.pos+2, value); err != nil {
			return 2, err
		}

		// write type code
		b.buffer[b.pos] = byte(ByteArray8Code.OpCode)

		// write length
		b.buffer[b.pos+1] = byte(size)

		wrote += size + 2
	} else if size < math.MaxUint16 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt8Array(b.buffer, b.pos+3, value); err != nil {
			return 3, err
		}
		// write type code
		b.buffer[b.pos] = byte(ByteArray16Code.OpCode)

		wrote += 3 + size
	} else if size < math.MaxUint32 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt8Array(b.buffer, b.pos+5, value); err != nil {
			return 5, err
		}
		// write type code
		b.buffer[b.pos] = byte(ByteArray32Code.OpCode)

		wrote += 5 + size
	} else {

		// write length
		if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt8Array(b.buffer, b.pos+9, value); err != nil {
			return 9, err
		}
		// write type code
		b.buffer[b.pos] = byte(ByteArray64Code.OpCode)

		wrote += 9 + size
	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

func (b *TupleBuilder) PutUint16Array(field string, value []uint16) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, Uint16ArrayField); err != nil {
		return 0, err
	}

	size := len(value)
	if size < math.MaxUint8 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16Array(b.buffer, b.pos+2, value); err != nil {
			return 2, err
		}

		// write type code
		b.buffer[b.pos] = byte(UnsignedShortArray8Code.OpCode)

		// write length
		b.buffer[b.pos+1] = byte(size)

		wrote += size + 2
	} else if size < math.MaxUint16 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutUint16Array(b.buffer, b.pos+3, value); err != nil {
			return 3, err
		}
		// write type code
		b.buffer[b.pos] = byte(UnsignedShortArray16Code.OpCode)

		wrote += 3 + size
	} else if size < math.MaxUint32 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutUint16Array(b.buffer, b.pos+5, value); err != nil {
			return 5, err
		}
		// write type code
		b.buffer[b.pos] = byte(UnsignedShortArray32Code.OpCode)

		wrote += 5 + size
	} else {

		// write length
		if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutUint16Array(b.buffer, b.pos+9, value); err != nil {
			return 9, err
		}
		// write type code
		b.buffer[b.pos] = byte(UnsignedShortArray64Code.OpCode)

		wrote += 9 + size
	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

func (b *TupleBuilder) PutInt16Array(field string, value []int16) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, Int16ArrayField); err != nil {
		return 0, err
	}

	size := len(value)
	if size < math.MaxUint8 {

		// write length
		if _, err = xbinary.LittleEndian.PutInt16Array(b.buffer, b.pos+2, value); err != nil {
			return 2, err
		}

		// write type code
		b.buffer[b.pos] = byte(ShortArray8Code.OpCode)

		// write length
		b.buffer[b.pos+1] = byte(size)

		wrote += size + 2
	} else if size < math.MaxUint16 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt16Array(b.buffer, b.pos+3, value); err != nil {
			return 3, err
		}
		// write type code
		b.buffer[b.pos] = byte(ShortArray16Code.OpCode)

		wrote += 3 + size
	} else if size < math.MaxUint32 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt16Array(b.buffer, b.pos+5, value); err != nil {
			return 5, err
		}
		// write type code
		b.buffer[b.pos] = byte(ShortArray32Code.OpCode)

		wrote += 5 + size
	} else {

		// write length
		if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt16Array(b.buffer, b.pos+9, value); err != nil {
			return 9, err
		}
		// write type code
		b.buffer[b.pos] = byte(ShortArray64Code.OpCode)

		wrote += 9 + size
	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

func (b *TupleBuilder) PutUint32Array(field string, value []uint32) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, Uint32ArrayField); err != nil {
		return 0, err
	}

	size := len(value)
	if size < math.MaxUint8 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32Array(b.buffer, b.pos+2, value); err != nil {
			return 2, err
		}

		// write type code
		b.buffer[b.pos] = byte(UnsignedIntArray8Code.OpCode)

		// write length
		b.buffer[b.pos+1] = byte(size)

		wrote += size + 2
	} else if size < math.MaxUint16 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutUint32Array(b.buffer, b.pos+3, value); err != nil {
			return 3, err
		}
		// write type code
		b.buffer[b.pos] = byte(UnsignedIntArray16Code.OpCode)

		wrote += 3 + size
	} else if size < math.MaxUint32 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutUint32Array(b.buffer, b.pos+5, value); err != nil {
			return 5, err
		}
		// write type code
		b.buffer[b.pos] = byte(UnsignedIntArray32Code.OpCode)

		wrote += 5 + size
	} else {

		// write length
		if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutUint32Array(b.buffer, b.pos+9, value); err != nil {
			return 9, err
		}
		// write type code
		b.buffer[b.pos] = byte(UnsignedIntArray64Code.OpCode)

		wrote += 9 + size
	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

func (b *TupleBuilder) PutInt32Array(field string, value []int32) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, Int32ArrayField); err != nil {
		return 0, err
	}

	size := len(value)
	if size < math.MaxUint8 {

		// write length
		if _, err = xbinary.LittleEndian.PutInt32Array(b.buffer, b.pos+2, value); err != nil {
			return 2, err
		}

		// write type code
		b.buffer[b.pos] = byte(IntArray8Code.OpCode)

		// write length
		b.buffer[b.pos+1] = byte(size)

		wrote += size + 2
	} else if size < math.MaxUint16 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt32Array(b.buffer, b.pos+3, value); err != nil {
			return 3, err
		}
		// write type code
		b.buffer[b.pos] = byte(IntArray16Code.OpCode)

		wrote += 3 + size
	} else if size < math.MaxUint32 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt32Array(b.buffer, b.pos+5, value); err != nil {
			return 5, err
		}
		// write type code
		b.buffer[b.pos] = byte(IntArray32Code.OpCode)

		wrote += 5 + size
	} else {

		// write length
		if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt32Array(b.buffer, b.pos+9, value); err != nil {
			return 9, err
		}
		// write type code
		b.buffer[b.pos] = byte(IntArray64Code.OpCode)

		wrote += 9 + size
	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

func (b *TupleBuilder) PutUint64Array(field string, value []uint64) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, Uint64ArrayField); err != nil {
		return 0, err
	}

	size := len(value)
	if size < math.MaxUint8 {

		// write data
		if _, err = xbinary.LittleEndian.PutUint64Array(b.buffer, b.pos+2, value); err != nil {
			return 2, err
		}

		// write type code
		b.buffer[b.pos] = byte(UnsignedLongArray8Code.OpCode)

		// write length
		b.buffer[b.pos+1] = byte(size)

		wrote += size + 2
	} else if size < math.MaxUint16 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutUint64Array(b.buffer, b.pos+3, value); err != nil {
			return 3, err
		}

		// write type code
		b.buffer[b.pos] = byte(UnsignedLongArray16Code.OpCode)

		wrote += 3 + size
	} else if size < math.MaxUint32 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutUint64Array(b.buffer, b.pos+5, value); err != nil {
			return 5, err
		}

		// write type code
		b.buffer[b.pos] = byte(UnsignedLongArray32Code.OpCode)

		wrote += 5 + size
	} else {
		// write length
		if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutUint64Array(b.buffer, b.pos+9, value); err != nil {
			return 9, err
		}

		// write type code
		b.buffer[b.pos] = byte(UnsignedLongArray64Code.OpCode)

		wrote += 9 + size
	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

func (b *TupleBuilder) PutInt64Array(field string, value []int64) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, Int64ArrayField); err != nil {
		return 0, err
	}

	size := len(value)
	if size < math.MaxUint8 {

		// write length
		if _, err = xbinary.LittleEndian.PutInt64Array(b.buffer, b.pos+2, value); err != nil {
			return 2, err
		}

		// write type code
		b.buffer[b.pos] = byte(LongArray8Code.OpCode)

		// write length
		b.buffer[b.pos+1] = byte(size)

		wrote += size + 2
	} else if size < math.MaxUint16 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt64Array(b.buffer, b.pos+3, value); err != nil {
			return 3, err
		}

		// write type code
		b.buffer[b.pos] = byte(LongArray16Code.OpCode)

		wrote += 3 + size
	} else if size < math.MaxUint32 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt64Array(b.buffer, b.pos+5, value); err != nil {
			return 5, err
		}

		// write type code
		b.buffer[b.pos] = byte(LongArray32Code.OpCode)

		wrote += 5 + size
	} else {

		// write length
		if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt64Array(b.buffer, b.pos+9, value); err != nil {
			return 9, err
		}

		// write type code
		b.buffer[b.pos] = byte(LongArray64Code.OpCode)

		wrote += 9 + size
	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

func (b *TupleBuilder) PutFloat32Array(field string, value []float32) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, Float32ArrayField); err != nil {
		return 0, err
	}

	size := len(value)
	if size < math.MaxUint8 {

		// write length
		if _, err = xbinary.LittleEndian.PutFloat32Array(b.buffer, b.pos+2, value); err != nil {
			return 2, err
		}

		// write type code
		b.buffer[b.pos] = byte(FloatArray8Code.OpCode)

		// write length
		b.buffer[b.pos+1] = byte(size)

		wrote += size + 2
	} else if size < math.MaxUint16 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutFloat32Array(b.buffer, b.pos+3, value); err != nil {
			return 3, err
		}

		// write type code
		b.buffer[b.pos] = byte(FloatArray16Code.OpCode)

		wrote += 3 + size
	} else if size < math.MaxUint32 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutFloat32Array(b.buffer, b.pos+5, value); err != nil {
			return 5, err
		}

		// write type code
		b.buffer[b.pos] = byte(FloatArray32Code.OpCode)

		wrote += 5 + size
	} else {

		// write length
		if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutFloat32Array(b.buffer, b.pos+9, value); err != nil {
			return 9, err
		}
		// write type code
		b.buffer[b.pos] = byte(FloatArray64Code.OpCode)

		wrote += 9 + size
	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

func (b *TupleBuilder) PutFloat64Array(field string, value []float64) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, Float64ArrayField); err != nil {
		return 0, err
	}

	size := len(value)
	if size < math.MaxUint8 {

		// write length
		if _, err = xbinary.LittleEndian.PutFloat64Array(b.buffer, b.pos+2, value); err != nil {
			return 2, err
		}

		// write type code
		b.buffer[b.pos] = byte(DoubleArray8Code.OpCode)

		// write length
		b.buffer[b.pos+1] = byte(size)

		wrote += size + 2
	} else if size < math.MaxUint16 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutFloat64Array(b.buffer, b.pos+3, value); err != nil {
			return 3, err
		}
		// write type code
		b.buffer[b.pos] = byte(DoubleArray16Code.OpCode)

		wrote += 3 + size
	} else if size < math.MaxUint32 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutFloat64Array(b.buffer, b.pos+5, value); err != nil {
			return 5, err
		}
		// write type code
		b.buffer[b.pos] = byte(DoubleArray32Code.OpCode)

		wrote += 5 + size
	} else {

		// write length
		if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutFloat64Array(b.buffer, b.pos+9, value); err != nil {
			return 9, err
		}
		// write type code
		b.buffer[b.pos] = byte(DoubleArray64Code.OpCode)

		wrote += 9 + size
	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

func (b *TupleBuilder) PutTimestampArray(field string, times []time.Time) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, DateArrayField); err != nil {
		return 0, err
	}

	// convert times to int64
	var value = make([]int64, len(times))
	for i := 0; i < len(times); i++ {
		value[i] = times[i].UnixNano()
	}

	size := len(value)
	if size < math.MaxUint8 {

		// write length
		if _, err = xbinary.LittleEndian.PutInt64Array(b.buffer, b.pos+2, value); err != nil {
			return 2, err
		}

		// write type code
		b.buffer[b.pos] = byte(TimestampArray8Code.OpCode)

		// write length
		b.buffer[b.pos+1] = byte(size)

		wrote += size + 2
	} else if size < math.MaxUint16 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt64Array(b.buffer, b.pos+3, value); err != nil {
			return 3, err
		}
		// write type code
		b.buffer[b.pos] = byte(TimestampArray16Code.OpCode)

		wrote += 3 + size
	} else if size < math.MaxUint32 {

		// write length
		if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt64Array(b.buffer, b.pos+5, value); err != nil {
			return 5, err
		}
		// write type code
		b.buffer[b.pos] = byte(TimestampArray32Code.OpCode)

		wrote += 5 + size
	} else {

		// write length
		if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size)); err != nil {
			return 1, err
		}

		// write value
		if _, err = xbinary.LittleEndian.PutInt64Array(b.buffer, b.pos+9, value); err != nil {
			return 9, err
		}
		// write type code
		b.buffer[b.pos] = byte(TimestampArray64Code.OpCode)

		wrote += 9 + size
	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

// func (b *TupleBuilder) PutTupleArray(field string, value []Tuple) (wrote int, err error) {
// 	return 0, nil
// 	// wrote, err = xbinary.LittleEndian.PutTupleArray(b.buffer, b.pos, value)
// 	// b.offsets[field] = b.pos
// 	// b.pos += wrote
// 	// return
// }

func (b *TupleBuilder) PutTupleArray(field string, value []Tuple) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, TupleArrayField); err != nil {
		return 0, err
	}

	// total size not including headers
	var totalSize int
	for i := 0; i < len(value); i++ {
		totalSize += value[i].Size()
	}

	for _, tuple := range value {

		size := tuple.Size()
		if size < math.MaxUint8 {

			// write length
			if written, err := tuple.Write(b.buffer[b.pos+2+wrote:]); err != nil {
				return 2 + written + wrote, err
			}

			// write type code
			b.buffer[b.pos+wrote] = byte(TupleArray8Code.OpCode)

			// write length
			b.buffer[b.pos+1+wrote] = byte(size)

			wrote += size + 2
		} else if size < math.MaxUint16 {

			// write length
			if _, err = xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1+wrote, uint16(size)); err != nil {
				return 1, err
			}

			// write type code
			b.buffer[b.pos+wrote] = byte(TimestampArray16Code.OpCode)

			// write value
			if written, err := tuple.Write(b.buffer[b.pos+3+wrote:]); err != nil {
				return 3 + written + wrote, err
			}

			wrote += 3 + size
		} else if size < math.MaxUint32 {

			// write length
			if _, err = xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1+wrote, uint32(size)); err != nil {
				return 1, err
			}

			// write value
			if written, err := tuple.Write(b.buffer[b.pos+5+wrote:]); err != nil {
				return 5 + written + wrote, err
			}
			// write type code
			b.buffer[b.pos+wrote] = byte(TimestampArray32Code.OpCode)

			wrote += 5 + size
		} else {

			// write length
			if _, err = xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1+wrote, uint64(size)); err != nil {
				return 1, err
			}

			// write value
			if written, err := tuple.Write(b.buffer[b.pos+9+wrote:]); err != nil {
				return 9 + written + wrote, err
			}
			// write type code
			b.buffer[b.pos+wrote] = byte(TimestampArray64Code.OpCode)

			wrote += 9 + size
		}

	}

	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

// func (b *TupleBuilder) PutStringArray(field string, value []string) (wrote int, err error) {
// 	return 0, nil
// 	// wrote, err = xbinary.LittleEndian.PutStringArray(b.buffer, b.pos, value)
// 	// b.offsets[field] = b.pos
// 	// b.pos += wrote
// 	// return
// }

func (b *TupleBuilder) Build() (Tuple, error) {
	defer b.reset()
	header, err := NewTupleHeader(*b)
	if err != nil {
		return NIL, err
	}
	return Tuple{data: b.buffer[:b.pos], Header: header}, nil
}
