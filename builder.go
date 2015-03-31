package namedtuple

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math"

	"github.com/eliquious/xbinary"
)

var syncHash SynchronizedHash = NewHasher(fnv.New32a())

// Empty Tuple
var NIL Tuple = Tuple{}

type TupleType struct {
	Namspace     string // Tuple Namespace
	Name         string // Tuple Name
	NamspaceHash uint32
	Hash         uint32
	versions     [][]Field
	fields       map[string]int
}

type Version struct {
	Num    uint8
	Fields []Field
}

type Field struct {
	Name     string
	Required bool
	Type     FieldType
}

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

func New(namespace string, name string) (t TupleType) {
	hash := syncHash.Hash([]byte(name))
	ns_hash := syncHash.Hash([]byte(namespace))
	t = TupleType{namespace, name, ns_hash, hash, make([][]Field, 0), make(map[string]int)}
	return
}

func (t *TupleType) AddVersion(fields ...Field) {
	t.versions = append(t.versions, fields)
	for _, field := range fields {
		t.fields[field.Name] = len(t.fields)
	}
}

func (t *TupleType) Contains(field string) bool {
	_, exists := t.fields[field]
	return exists
}

func (t *TupleType) Offset(field string) (offset int, exists bool) {
	offset, exists = t.fields[field]
	return
}

func (t *TupleType) NumVersions() int {
	return len(t.versions)
}

func (t *TupleType) Versions() (vers []Version) {
	vers = make([]Version, t.NumVersions())
	for i := 0; i < t.NumVersions(); i++ {
		vers[i] = Version{uint8(i + 1), t.versions[i]}
	}
	return
}

// type ReferenceField struct {
// 	Field
// 	ReferenceType string
// }

func NewTupleHeader(b TupleBuilder) (TupleHeader, error) {

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
		NamespaceHash:   b.tupleType.NamspaceHash,
		Hash:            b.tupleType.Hash,
		FieldCount:      totalFieldCount,
		FieldSize:       fieldSize,
		ContentLength:   uint64(b.pos),
		Offsets:         offsets,
		Type:            b.tupleType,
	}, nil
}

type TupleHeader struct {
	// protocol version  (+1) - {uint8}
	// tuple version     (+1) - {uint8}
	// namespace hash    (+4) - {uint32}
	// hash code         (+4) - {uint32}
	// field count       (+4) - {uint32}
	// field size        (+1) - {1,2,4,8} bytes
	//  - fields -            (field count * field size)
	// data length       (+8) - {depends on field size (ie. same as field size)}
	ProtocolVersion uint8
	TupleVersion    uint8
	NamespaceHash   uint32
	Hash            uint32
	FieldCount      uint32
	FieldSize       uint8
	ContentLength   uint64
	Offsets         []uint64
	Type            TupleType
}

func (t *TupleHeader) Size() int {

	// data size width is the same as the field size
	size := 15 + int(t.FieldSize)*int(t.FieldCount) + int(t.FieldSize)
	return size
}

func (t *TupleHeader) Write(dst []byte) (int, error) {

	if len(dst) < t.Size() {
		return 0, xbinary.ErrOutOfRange
	} else if len(t.Offsets) != int(t.FieldCount) {
		return 0, errors.New("Invalid Header: Field count does not equal number of field offsets")
	}

	// copy([]byte("ENT"), dst)
	dst[0] = byte(t.ProtocolVersion)
	dst[1] = byte(t.TupleVersion)
	binary.LittleEndian.PutUint32(dst[2:], t.NamespaceHash)
	binary.LittleEndian.PutUint32(dst[6:], t.Hash)
	binary.LittleEndian.PutUint32(dst[10:], t.FieldCount)
	dst[14] = byte(t.FieldSize)

	pos := 15
	switch t.FieldSize {
	case 1:
		for _, offset := range t.Offsets {
			dst[pos] = byte(offset)
			pos++
		}
		dst[pos] = byte(t.ContentLength)
	case 2:
		for _, offset := range t.Offsets {
			binary.LittleEndian.PutUint16(dst[pos:], uint16(offset))
			pos += 2
		}
		binary.LittleEndian.PutUint16(dst[pos:], uint16(t.ContentLength))
	case 4:
		for _, offset := range t.Offsets {
			binary.LittleEndian.PutUint32(dst[pos:], uint32(offset))
			pos += 4
		}
		binary.LittleEndian.PutUint32(dst[pos:], uint32(t.ContentLength))
	case 8:
		for _, offset := range t.Offsets {
			binary.LittleEndian.PutUint64(dst[pos:], offset)
			pos += 8
		}
		binary.LittleEndian.PutUint64(dst[pos:], t.ContentLength)
	default:
		return pos, errors.New("Invalid Header: Field size must be 1,2,4 or 8 bytes")
	}
	pos += int(t.FieldSize)
	return pos, nil
}

type Tuple struct {
	data   []byte
	Header TupleHeader
}

func (t *Tuple) Is(tupleType TupleType) bool {
	return t.Header.Hash == tupleType.Hash
}

func (t *Tuple) Write(data []byte) (int, error) {
	if (t.Size() + t.Header.Size()) > len(data) {
		return 0, xbinary.ErrOutOfRange
	}

	// write header
	var wrote int
	if wrote, err := t.Header.Write(data); err != nil {
		return wrote, nil
	}

	wrote += copy(data[wrote:], t.data)
	return wrote, nil
}

func (t *Tuple) Size() int {
	return len(t.data)
}

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
