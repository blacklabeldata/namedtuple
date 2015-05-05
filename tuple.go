package namedtuple

import (
	"fmt"
	"math"

	"github.com/swiftkick-io/xbinary"
)

// PutTuple writes a tuple into the given field. The field type must be a TupleField, otherwise an error will be returned. The type code is written first, then the length, then the value. If the tuple length is less than `math.MaxUint8`, a single byte is used to represent the length. If the tuple length is less than `math.MaxUint16`, an unsigned 16-bit integer is used to represent the length and so on as the length increases. If the buffer is not large enough to store the entire tuple an `xbinary.ErrOutOfRange` error is returned. If the write is successful, the number of bytes written is returned as well as a nil error.
func (b *TupleBuilder) PutTuple(field string, value Tuple) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, TupleField); err != nil {
		return 0, err
	}

	size := value.Size()
	if size < math.MaxUint8 {

		// check length
		if b.available() < size+2 {
			return 0, xbinary.ErrOutOfRange
		}

		// write tuple
		wrote, err = value.Write(b.buffer[b.pos+2:])

		// write type code
		b.buffer[b.pos] = byte(Tuple8Code.OpCode)

		// write length
		b.buffer[b.pos+1] = byte(size)

		wrote += 2

	} else if size < math.MaxUint16 {

		// check length
		if b.available() < size+3 {
			return 0, xbinary.ErrOutOfRange
		}

		// write length
		xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1, uint16(size))

		// write type code
		b.buffer[b.pos] = byte(Tuple16Code.OpCode)

		// write tuple
		wrote, err = value.Write(b.buffer[b.pos+2:])
		wrote += 3
	} else if size < math.MaxUint32 {

		// check length
		if b.available() < size+5 {
			return 0, xbinary.ErrOutOfRange
		}

		// write length
		xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1, uint32(size))

		// write type code
		b.buffer[b.pos] = byte(Tuple32Code.OpCode)

		// write tuple
		wrote, err = value.Write(b.buffer[b.pos+5:])
		wrote += 5

	} else {

		// check length
		if b.available() < size+9 {
			return 0, xbinary.ErrOutOfRange
		}

		// write length
		xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1, uint64(size))

		// write type code
		b.buffer[b.pos] = byte(Tuple64Code.OpCode)

		// write tuple
		wrote, err = value.Write(b.buffer[b.pos+9:])
		wrote += 9
	}

	// store offset and increment position
	b.offsets[field] = b.pos
	b.pos += wrote
	return
}

// PutTupleArray writes an array of tuples for the given field. The field type must be `TupleArrayField`, otherwise an error will be returned.
func (b *TupleBuilder) PutTupleArray(field string, value []Tuple) (wrote int, err error) {

	// field type should be
	if err = b.typeCheck(field, TupleArrayField); err != nil {
		return 0, err
	}

	// calculate total size
	var totalSize int
	var tupleSize int
	for i := 0; i < len(value); i++ {
		tupleSize = value[i].Size()

		// add tuple header
		if tupleSize < math.MaxUint8 {
			totalSize += 2
		} else if tupleSize < math.MaxUint16 {
			totalSize += 3
		} else if tupleSize < math.MaxUint32 {
			totalSize += 5
		} else {
			totalSize += 9
		}

		// add tuple size
		totalSize += tupleSize
	}

	// return error if buffer is too small
	// 8-bit total size check
	if totalSize < math.MaxUint8 && b.available() < totalSize+2 {
		return 0, xbinary.ErrOutOfRange

		// 16-bit total size check
	} else if totalSize < math.MaxUint16 && b.available() < totalSize+3 {
		return 0, xbinary.ErrOutOfRange

		// 32-bit total size check
	} else if totalSize < math.MaxUint32 && b.available() < totalSize+5 {
		return 0, xbinary.ErrOutOfRange

		// 64-bit size check
	} else if totalSize > math.MaxUint32 && b.available() < totalSize+9 {
		return 0, xbinary.ErrOutOfRange
	}

	// write array values
	for _, tuple := range value {

		size := tuple.Size()
		if size < math.MaxUint8 {

			// check length
			if b.available() < size+2 {
				return wrote, xbinary.ErrOutOfRange
			}

			// write length
			tuple.Write(b.buffer[b.pos+2+wrote:])

			// write type code
			b.buffer[b.pos+wrote] = byte(TupleArray8Code.OpCode)

			// write length
			b.buffer[b.pos+1+wrote] = byte(size)

			wrote += size + 2
		} else if size < math.MaxUint16 {
			// check length
			if b.available() < size+3 {
				return wrote, xbinary.ErrOutOfRange
			}

			// write length
			xbinary.LittleEndian.PutUint16(b.buffer, b.pos+1+wrote, uint16(size))

			// write type code
			b.buffer[b.pos+wrote] = byte(TimestampArray16Code.OpCode)

			// write value
			if written, err := tuple.Write(b.buffer[b.pos+3+wrote:]); err != nil {
				return 3 + written + wrote, err
			} else if written != size {
				err = fmt.Errorf("Incorrect tuple length: expected %d, actual %d", size, written)
				return wrote, err
			}

			wrote += 3 + size
		} else if size < math.MaxUint32 {

			// check length
			if b.available() < size+5 {
				return wrote, xbinary.ErrOutOfRange
			}

			// write length
			xbinary.LittleEndian.PutUint32(b.buffer, b.pos+1+wrote, uint32(size))

			// write value
			if written, err := tuple.Write(b.buffer[b.pos+5+wrote:]); err != nil {
				return 5 + written + wrote, err
			} else if written != size {
				err = fmt.Errorf("Incorrect tuple length: expected %d, actual %d", size, written)
				return wrote, err
			}

			// write type code
			b.buffer[b.pos+wrote] = byte(TimestampArray32Code.OpCode)

			wrote += 5 + size
		} else {

			// write length
			xbinary.LittleEndian.PutUint64(b.buffer, b.pos+1+wrote, uint64(size))

			// write value
			if written, err := tuple.Write(b.buffer[b.pos+9+wrote:]); err != nil {
				return 9 + written + wrote, err
			} else if written != size {
				err = fmt.Errorf("Incorrect tuple length: expected %d, actual %d", size, written)
				return wrote, err
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
