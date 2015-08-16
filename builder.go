package namedtuple

import (
	"bytes"
	"errors"
	"math"
)

type TupleBuilder struct {
	fields    map[string]Field
	offsets   map[string]int
	tupleType TupleType
	buffer    []byte
	pos       int
}

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

func (b *TupleBuilder) Build() (Tuple, error) {
	defer b.reset()
	header, err := b.newTupleHeader()
	if err != nil {
		return NIL, err
	}
	return Tuple{data: b.buffer[:b.pos], Header: header}, nil
}

func (b *TupleBuilder) newTupleHeader() (TupleHeader, error) {

	// validation of required fields
	var tupleVersion uint8
	var missingField string
	var fieldSize uint8
	var fieldCount int

	totalFieldCount := uint32(len(b.tupleType.fields))
	offsets := make([]uint64, totalFieldCount)

	// iterate over all the versions
	for _, version := range b.tupleType.Versions() {
	OUTER:

		// iterate over all the fields for the current version
		for _, field := range version.Fields {

			// get offset for field
			offset, exists := b.offsets[field.Name]

			// if the field is required, determine if it has been added to the builder
			if field.Required {

				// if the field has not been written
				// exit the loop and save the missing field name
				if !exists {
					missingField = field.Name
					break OUTER
				}

				// set byte offset of field in tuple data
				offsets[fieldCount] = uint64(offset)
			} else {

				// if the optional fields was not written, encode a maximum offset
				if !exists {

					// set byte offset of field in tuple data
					offsets[fieldCount] = uint64(math.MaxUint64)

				} else {
					// if the optional field does exist
					// set byte offset of field in tuple data
					offsets[fieldCount] = uint64(offset)
				}
			}
		}

		// increment the version number after all required fields have been satisfied
		tupleVersion++
	}

	// If the first version is missing a field, return an error
	// At least one version must contain all the required fields.
	// The version number will increment for each version which
	// contains all the required fields.
	if tupleVersion < 1 {
		return TupleHeader{}, errors.New("Missing required field: " + missingField)
	}

	// TODO: Add Field level validation

	// Calculate minimum offset for accessing all fields in data
	// If the total data size is < 256 bytes, all field offsets
	if b.pos < math.MaxUint8-1 {
		fieldSize = 1
	} else if b.pos < math.MaxUint16 {
		fieldSize = 2
	} else if b.pos < math.MaxUint32 {
		fieldSize = 4
	} else {
		fieldSize = 8
	}

	return TupleHeader{
		ProtocolVersion: 0,
		TupleVersion:    tupleVersion,
		NamespaceHash:   b.tupleType.NamespaceHash,
		Hash:            b.tupleType.Hash,
		FieldCount:      totalFieldCount,
		FieldSize:       fieldSize,
		ContentLength:   uint64(b.pos),
		Offsets:         offsets,
		Type:            b.tupleType,
	}, nil
}

// type protocolHeader []byte

// func (p protocolHeader) ProtocolVersion() (v uint8, err error) {
// 	if len(p) > 0 {
// 		v = p[0] & ProtocolVersionMask
// 	} else {
// 		err = xbinary.ErrOutOfRange
// 	}
// 	return
// }

// func (p protocolHeader) ContentLength() (l uint64, err error) {
// 	var sizeEnum byte
// 	if len(p) > 0 {
// 		sizeEnum = p[0] & ProtocolSizeEnumMask >> 6
// 	} else {
// 		err = xbinary.ErrOutOfRange
// 		return
// 	}

// 	switch sizeEnum {
// 	case 0:
// 		if len(p) > 1 {
// 			l = uint64(p[1])
// 		} else {
// 			err = xbinary.ErrOutOfRange
// 		}

// 	case 1:
// 		if size, e := xbinary.LittleEndian.Uint16(p, 1); err == nil {
// 			l = uint64(size)
// 		} else {
// 			err = e
// 		}

// 	case 2:
// 		if size, e := xbinary.LittleEndian.Uint32(p, 1); err == nil {
// 			l = uint64(size)
// 		} else {
// 			err = e
// 		}

// 	case 3:
// 		if size, e := xbinary.LittleEndian.Uint64(p, 1); err == nil {
// 			l = uint64(size)
// 		} else {
// 			err = e
// 		}
// 	}
// 	return
// }

// type packetHeader []byte

// func (p packetHeader) ProtocolVersion() (v uint8, err error) {
// 	if len(p) > 0 {
// 		v = p[0]
// 	} else {
// 		err = xbinary.ErrOutOfRange
// 	}
// 	return
// }

// func (p packetHeader) TupleVersion() (v uint8, err error) {
// 	if len(p) > 1 {
// 		v = p[1]
// 	} else {
// 		err = xbinary.ErrOutOfRange
// 	}
// 	return
// }

// func (p packetHeader) NamespaceHash() (uint32, error) {
// 	return xbinary.LittleEndian.Uint32(p, 2)
// }

// func (p packetHeader) TypeHash() (uint32, error) {
// 	return xbinary.LittleEndian.Uint32(p, 6)
// }

// func (p packetHeader) NumberOfFields() (uint32, error) {
// 	return xbinary.LittleEndian.Uint32(p, 10)
// }

// func (p packetHeader) OffsetSize() (v uint8, err error) {
// 	if len(p) > 15 {
// 		v = p[15]
// 	} else {
// 		err = xbinary.ErrOutOfRange
// 	}
// 	return
// }

// func (p packetHeader) ContentLength() (l uint64, err error) {

// 	// Get size of field offsets
// 	size, e := p.OffsetSize()
// 	if e != nil {
// 		err = e
// 		return
// 	}

// 	// Get number of fields
// 	count, e := p.NumberOfFields()
// 	if e != nil {
// 		err = e
// 		return
// 	}
// 	return xbinary.LittleEndian.Uint64(p, 16+int(size)*int(count))
// }

// func (p packetHeader) Offsets() (offsets []uint64, err error) {

// 	// Get size of field offsets
// 	size, e := p.OffsetSize()
// 	if e != nil {
// 		err = e
// 		return
// 	}

// 	// Get number of fields
// 	count, e := p.NumberOfFields()
// 	if e != nil {
// 		err = e
// 		return
// 	}

// 	// Resize offsets
// 	offsets = make([]uint64, int(count))

// 	if len(p) < 16+int(size)*int(count) {
// 		err = xbinary.ErrOutOfRange
// 		return
// 	}

// 	// Get offsets
// 	switch size {
// 	case 1:
// 		for i, o := range p[16 : 16+int(count)] {
// 			offsets[i] = uint64(o)
// 		}
// 	case 2:
// 	case 4:
// 	case 8:
// 	default:
// 		err = fmt.Errorf("")
// 	}
// 	return
// }

type Tuple struct {
	data   []byte
	Header TupleHeader
}

func (t *Tuple) Is(tupleType TupleType) bool {
	return t.Header.Hash == tupleType.Hash && t.Header.NamespaceHash == tupleType.NamespaceHash
}

// Size returns the number of bytes used to store the tuple data
func (t *Tuple) Size() int {
	return len(t.data)
}

// Offset returns the byte offset for the given field
func (t *Tuple) Offset(field string) (int, error) {
	index, exists := t.Header.Type.Offset(field)
	if !exists {
		return 0, errors.New("Field does not exist")
	}

	// Tuple type and tuple header do not agree on fields
	if index >= int(t.Header.FieldCount) {
		return 0, errors.New("Invalid field index")
	}
	return int(t.Header.Offsets[index]), nil
}

// Payload returns the bytes representing the tuple. The tuple header is not included
func (t *Tuple) Payload() []byte {
	return t.data
}

// WriteAt writes the tuple into the given byte array at the given offset.
func (t *Tuple) WriteAt(p []byte, off int64) (n int, err error) {

	size := t.Header.Size()
	if int64(len(p))-off < int64(size+len(t.data)) {
		return 0, errors.New("Buffer too small")
	}

	// Write header
	buf := bytes.NewBuffer(p[off:])
	if written, err := t.Header.WriteTo(buf); err == nil {
		n += int(written)
	} else {
		return 0, err
	}

	// Copy payload
	var offset = int(off) + n
	copy(p[offset:], t.data)

	return
}
