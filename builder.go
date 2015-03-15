package namedtuple

import (
	"errors"
	"math"

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
