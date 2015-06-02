package namedtuple

import (
	"encoding/binary"
	"errors"
)

// skip proto version  (+1) - {uint8}
// skip tuple version  (+1) - {uint8}
// skip namespace code (+4) - {uint32}
// skip hash code      (+4) - {uint32}
// skip field count    (+4) - {uint32}
// skip field size     (+1) - {1,2,4,8} bytes
//  - fields -         (field count * field size)
// skip data length    (+8) - {depends on field size (ie. same as field size)}
//
func Decode(r Registry, data []byte) (Tuple, error) {

	// fail fast - minimum fixed header size is 14
	if len(data) < 15 {
		return NIL, errors.New("Invalid Header: Too small")
	}

	header := TupleHeader{}
	header.ProtocolVersion = uint8(data[0])
	header.TupleVersion = uint8(data[1])
	header.NamespaceHash = binary.LittleEndian.Uint32(data[2:])
	header.Hash = binary.LittleEndian.Uint32(data[6:])

	// attach tuple type
	// var tupleType TupleType
	tupleType, exists := r.GetWithHash(header.NamespaceHash, header.Hash)
	if !exists {
		return NIL, errors.New("Unknown tuple type")
	}
	header.Type = tupleType

	// fields
	header.FieldCount = binary.LittleEndian.Uint32(data[10:])
	header.FieldSize = uint8(data[14])

	// now we know how large the full header is with field offsets and data length
	fullHeaderSize := 15 + int(header.FieldCount)*int(header.FieldSize) + int(header.FieldSize)
	if len(data) < fullHeaderSize {
		return NIL, errors.New("Invalid Header: Too small")
	}

	// current position
	pos := 15

	// decoding field offsets
	header.Offsets = make([]uint64, header.FieldCount)
	switch header.FieldSize {
	case 1:

		for i := 0; i < int(header.FieldCount); i++ {
			header.Offsets[i] = uint64(data[pos])
			pos++
		}
		header.ContentLength = uint64(data[pos])
	case 2:
		for i := 0; i < int(header.FieldCount); i++ {
			header.Offsets[i] = uint64(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2
		}
		header.ContentLength = uint64(binary.LittleEndian.Uint16(data[pos:]))
	case 4:
		for i := 0; i < int(header.FieldCount); i++ {
			header.Offsets[i] = uint64(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
		}
		header.ContentLength = uint64(binary.LittleEndian.Uint32(data[pos:]))
	case 8:
		for i := 0; i < int(header.FieldCount); i++ {
			header.Offsets[i] = uint64(binary.LittleEndian.Uint64(data[pos:]))
			pos += 8
		}
		header.ContentLength = uint64(binary.LittleEndian.Uint64(data[pos:]))
	default:
		return NIL, errors.New("Invalid field length")
	}

	pos += int(header.FieldSize)
	if int(header.ContentLength) != (len(data) - pos) {
		return NIL, errors.New("Invalid header: incorrect content length")
	}

	return Tuple{data: data[pos:], Header: header}, nil
}
