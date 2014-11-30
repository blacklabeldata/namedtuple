package namedtuple

import (
	// "fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTupleTypeNew(t *testing.T) {

	User := New("User")

	assert.Equal(t, "User", User.Name)
	assert.Equal(t, 0, User.NumVersions())

	hash := syncHash.Hash([]byte("User"))
	assert.Equal(t, hash, User.Hash)
}

func TestTupleTypeAddVersion(t *testing.T) {

	// fields
	uuid := Field{"uuid", true, StringField}
	username := Field{"username", true, StringField}
	age := Field{"age", false, Uint8Field}
	location := Field{"location", false, TupleField}

	// create tuple type
	User := New("user")
	User.AddVersion(uuid, username, age)
	User.AddVersion(location)

	// verify versions were added
	vs := User.Versions()
	assert.Equal(t, 2, User.NumVersions())
	assert.Equal(t, len(vs), User.NumVersions())

	// verify fields
	// version 1
	assert.Equal(t, 1, int(vs[0].Num))
	assert.Equal(t, uuid, vs[0].Fields[0])
	assert.Equal(t, username, vs[0].Fields[1])
	assert.Equal(t, age, vs[0].Fields[2])

	// version 2
	assert.Equal(t, 2, int(vs[1].Num))
	assert.Equal(t, location, vs[1].Fields[0])
}

func TestTupleTypeFieldOffset(t *testing.T) {

	// fields
	uuid := Field{"uuid", true, StringField}
	username := Field{"username", true, StringField}
	age := Field{"age", false, Uint8Field}
	location := Field{"location", false, TupleField}

	// create tuple type
	User := New("user")
	User.AddVersion(uuid, username, age)
	User.AddVersion(location)

	// uuid field
	offset, exists := User.Offset("uuid")
	assert.Equal(t, 0, offset)
	assert.Equal(t, true, exists)

	// username field
	offset, exists = User.Offset("username")
	assert.Equal(t, 1, offset)
	assert.Equal(t, true, exists)

	// age field
	offset, exists = User.Offset("age")
	assert.Equal(t, 2, offset)
	assert.Equal(t, true, exists)

	// location field
	offset, exists = User.Offset("location")
	assert.Equal(t, 3, offset)
	assert.Equal(t, true, exists)

	// bad field
	offset, exists = User.Offset("bad")
	assert.Equal(t, 0, offset)
	assert.Equal(t, false, exists)
}
