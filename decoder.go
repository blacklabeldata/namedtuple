package namedtuple

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/swiftkick-io/xbinary"
)

var (
	// ErrTupleExceedsMaxSize is returned if the length of a Tuple of greater than the maximum allowable size
	// for the Decoder.
	ErrTupleExceedsMaxSize = fmt.Errorf("Tuple exceeds maximum allowable length")

	// ErrInvalidProtocolVersion is returned from Decode() if the Tuple version is unknown.
	ErrInvalidProtocolVersion = fmt.Errorf("Invalid protocol version in Tuple header")

	// ErrTupleLengthTooSmall is returned from the Decode() method if the decoded length is too small to include all the required information
	ErrTupleLengthTooSmall = fmt.Errorf("Tuple length is too short to include all the required information")

	ErrUnknownTupleType = fmt.Errorf("Unknown tuple type")

	// EmptyTuple is returned along with an error from the Decode() method.
	EmptyTuple = Tuple{}
)

const (
	VersionOneTupleHeaderSize = 13
)

// Create a reader which reads the first byte and the content length.
// If the length exceeds the maxSize, return an error
// Create a bytes.Buffer and io.CopyN(contentLength) into the buffer
// Based on the protocol version, decode(buffer.Bytes()) into (Tuple, error)

// decoder := NewDecoder(reg, 65536)
// for _, tup, err := decoder.Decode(reader); err != nil {
// }

type Decoder interface {
	Decode() (Tuple, error)
}

func NewDecoder(reg Registry, maxSize uint64, r io.Reader) Decoder {
	var buf []byte
	return decoder{reg, maxSize, bytes.NewBuffer(buf), bufio.NewReader(r)}
}

type decoder struct {
	reg     Registry
	maxSize uint64
	buffer  *bytes.Buffer
	reader  *bufio.Reader
}

func (d decoder) Decode() (Tuple, error) {

	pH, err := d.reader.ReadByte()
	if err != nil {
		return EmptyTuple, err
	}

	// Parse nuber of length bytes and version
	byteCount, version := ParseProtocolHeader(pH)

	// Read bytes for content length
	b, err := d.reader.Peek(int(byteCount))
	if err != nil {
		return EmptyTuple, err
	}

	// Parse content length based on number of bytes
	length, err := d.parseLength(byteCount, b)
	if err != nil {
		return EmptyTuple, err
	}

	// Verify length against maxSize
	if length > d.maxSize {
		return EmptyTuple, ErrTupleExceedsMaxSize
	}

	// Copy Length bytes into buffer
	if _, err := io.CopyN(d.buffer, d.reader, int64(length)); err != nil {
		return EmptyTuple, err
	}

	// Depending on the protocol version, parse the tuple
	switch version {
	case 0:
		return d.parseVersionOneTuple(byteCount, version, length)
	default:
		return EmptyTuple, ErrInvalidProtocolVersion
	}

	return EmptyTuple, nil
}

func (d decoder) parseLength(byteCount uint8, buf []byte) (l uint64, err error) {
	switch byteCount {
	case 1:
		if len(buf) == 1 {
			l = uint64(buf[0])
		} else {
			err = xbinary.ErrOutOfRange
		}
	case 2:
		if size, e := xbinary.LittleEndian.Uint16(buf, 0); err == nil {
			l = uint64(size)
		} else {
			err = e
		}
	case 4:
		if size, e := xbinary.LittleEndian.Uint32(buf, 0); err == nil {
			l = uint64(size)
		} else {
			err = e
		}
	case 8:
		if size, e := xbinary.LittleEndian.Uint64(buf, 0); err == nil {
			l = uint64(size)
		} else {
			err = e
		}
	}
	return
}

func (d decoder) parseVersionOneTuple(offsetBytes uint8, protocolVersion uint8, length uint64) (t Tuple, err error) {
	buffer := d.buffer.Bytes()
	var namespaceHash, typeHash, fieldCount uint32
	var version uint8

	// The buffer needs to be at least 13 bytes. This includes the uint8 tuple version, the uint32 namespace and type hashes and the field count
	if len(buffer) < VersionOneTupleHeaderSize {
		return EmptyTuple, ErrTupleLengthTooSmall
	}

	// Read Tuple version
	version = buffer[0]

	// Read namespace hash
	namespaceHash, err = xbinary.LittleEndian.Uint32(buffer, 1)
	if err != nil {
		return EmptyTuple, err
	}

	// Read type hash
	typeHash, err = xbinary.LittleEndian.Uint32(buffer, 5)
	if err != nil {
		return EmptyTuple, err
	}

	// Check if known tuple type
	tupleType, exists := d.reg.GetWithHash(namespaceHash, typeHash)
	if !exists {
		return EmptyTuple, err
	}

	// Read field count
	fieldCount, err = xbinary.LittleEndian.Uint32(buffer, 9)
	if err != nil {
		return EmptyTuple, err
	}

	offsets := make([]uint64, int(fieldCount))
	switch offsetBytes {
	case 1:
		// Check buffer length
		if len(buffer) < int(fieldCount)+VersionOneTupleHeaderSize {
			return EmptyTuple, ErrTupleLengthTooSmall
		}

		// Process offsets
		for i := VersionOneTupleHeaderSize; i < int(fieldCount)+VersionOneTupleHeaderSize; i++ {
			offsets[i-VersionOneTupleHeaderSize] = uint64(buffer[i])
		}
	case 2:
		o := make([]uint16, int(fieldCount))
		err = xbinary.LittleEndian.Uint16Array(buffer, VersionOneTupleHeaderSize, &o)
		if err == nil {
			for i, offset := range o {
				offsets[i] = uint64(offset)
			}
		}
	case 4:
		o := make([]uint32, int(fieldCount))
		err = xbinary.LittleEndian.Uint32Array(buffer, VersionOneTupleHeaderSize, &o)
		if err == nil {
			for i, offset := range o {
				offsets[i] = uint64(offset)
			}
		}
	case 8:
		o := make([]uint64, int(fieldCount))
		err = xbinary.LittleEndian.Uint64Array(buffer, VersionOneTupleHeaderSize, &o)
		if err == nil {
			for i, offset := range o {
				offsets[i] = uint64(offset)
			}
		}
	}

	// Create TupleHeader
	t.Header = TupleHeader{
		ProtocolVersion: protocolVersion,
		TupleVersion:    version,
		NamespaceHash:   namespaceHash,
		Hash:            typeHash,
		FieldCount:      fieldCount,
		FieldSize:       offsetBytes,
		ContentLength:   length,
		Offsets:         offsets,
		Type:            tupleType,
	}

	// Slice tuple data
	pos := VersionOneTupleHeaderSize + int(fieldCount)*int(offsetBytes)
	t.data = buffer[pos:]
	return
}

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
