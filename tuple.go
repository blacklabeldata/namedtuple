package namedtuple

import (
	"encoding/binary"
	"errors"
	"github.com/eliquious/xbinary"
	"hash/fnv"
	"math"
	// "sync"
)

var syncHash SynchronizedHash = NewHasher(fnv.New32a())

// Empty Tuple
var NIL Tuple = Tuple{}

func New(name string) (t TupleType) {
	hash := syncHash.Hash([]byte(name))
	t = TupleType{name, hash, make([][]Field, 0), make(map[string]int)}
	return
}

type TupleType struct {
	Name     string // Tuple Name
	Hash     uint32
	versions [][]Field
	fields   map[string]int
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

type Version struct {
	Num    uint8
	Fields []Field
}

type Field struct {
	Name     string
	Required bool
	Type     FieldType
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
		Hash:            b.tupleType.Hash,
		FieldCount:      totalFieldCount,
		FieldSize:       fieldSize,
		ContentLength:   uint64(b.pos),
		Offsets:         offsets,
		Type:            b.tupleType,
	}, nil
}

type TupleHeader struct {
	// skip ENT version    (+1) - {uint8}
	// skip tuple version  (+1) - {uint8}
	// skip hash code      (+4) - {uint32}
	// skip field count    (+4) - {uint32}
	// skip field size     (+1) - {1,2,4,8} bytes
	//  - fields -         (field count * field size)
	// skip data length    (+8) - {depends on field size (ie. same as field size)}
	ProtocolVersion uint8
	TupleVersion    uint8
	Hash            uint32
	FieldCount      uint32
	FieldSize       uint8
	ContentLength   uint64
	Offsets         []uint64
	Type            TupleType
}

func (t *TupleHeader) Size() int {

	// data size width is the same as the field size
	size := 11 + int(t.FieldSize)*int(t.FieldCount) + int(t.FieldSize)
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
	binary.LittleEndian.PutUint32(dst[2:], t.Hash)
	binary.LittleEndian.PutUint32(dst[6:], t.FieldCount)
	dst[10] = byte(t.FieldSize)

	pos := 11
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

// func main() {

// 	User := namedtuple.New("user")
// 	// User.AddVersion(namedtuple.NewVersion(1).
// 	// 	AddField("uuid", true, namedtuple.StringField).
// 	// 	AddField("username", true, namedtuple.StringField).
// 	// 	AddField("age", false, namedtuple.Uint8))

// 	User.AddVersion(
// 		Field{"uuid", true, namedtuple.StringField},
// 		Field{"username", true, namedtuple.StringField},
// 		Field{"age", false, namedtuple.Uint8},
// 	)
// 	User.AddVersion(
// 		Field{"location", false, namedtuple.TupleField, "location"},
// 	)

// 	Location := namedtuple.New("location")
// 	Location.AddVersion(
// 		Field{"address", true, namedtuple.StringField},
// 		Field{"city", true, namedtuple.StringField},
// 		Field{"suite", false, namedtuple.StringField},
// 		Field{"zip", true, namedtuple.Uint32},
// 		Field{"country", true, namedtuple.StringField},
// 		Field{"providence", true, namedtuple.StringField},
// 	)

// 	loc_builder := Location.Builder()
// 	loc_builder.PutString("address", "129 Appleberry Lane")
// 	loc_builder.PutString("city", "Harvest")
// 	loc_builder.PutUint32("zip", 35749)
// 	loc_builder.PutString("country", "US")
// 	loc_builder.PutString("providence", "AL")
// 	loc := loc_builder.Build()

// 	user_builder := User.Builder()

// 	err := user_builder.PutString("uuid", "13098230498203984098234")
// 	err = user_builder.PutString("username", "max.franks")
// 	err = user_builder.PutUint8("age", 29)
// 	err = user_builder.PutTuple("location", loc)

// 	u, err := user_builder.Build()
// 	u.Write(os.StdOut)

// 	uuid, err := u.GetString("uuid")
// 	username, err := u.GetString("uuid")

// }
