package namedtuple

import (
	// "fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	// "time"
)

func createTestTupleType() TupleType {
	// fields
	uuid := Field{"uuid", true, StringField}
	username := Field{"username", true, StringField}
	age := Field{"age", false, Uint8Field}
	location := Field{"location", false, TupleField}

	// create tuple type
	User := New("user")
	User.AddVersion(uuid, username, age)
	User.AddVersion(location)
	return User
}

func TestRegistry(t *testing.T) {

	// create new empty registry
	reg := NewRegistry()

	// make sure it's empty
	assert.Equal(t, 0, reg.Size())
	assert.Equal(t, len(reg.content), reg.Size())
}

func TestRegistryRegister(t *testing.T) {

	// create new empty registry
	reg := NewRegistry()

	// create type
	User := createTestTupleType()

	// add User type
	reg.Register(User)

	// make sure it's not empty
	assert.Equal(t, 1, reg.Size())
	assert.Equal(t, len(reg.content), reg.Size())
}

func TestRegistryUnregister(t *testing.T) {

	// create new empty registry
	reg := NewRegistry()

	// create type
	User := createTestTupleType()

	// add User type
	reg.Register(User)

	// make sure it's not empty
	assert.Equal(t, 1, reg.Size())
	assert.Equal(t, len(reg.content), reg.Size())

	// remove User type
	reg.Unregister(User)

	// make sure it's not empty
	assert.Equal(t, 0, reg.Size())
	assert.Equal(t, len(reg.content), reg.Size())
}

func TestRegistryContainsTrue(t *testing.T) {

	// create new empty registry
	reg := NewRegistry()

	// create type
	User := createTestTupleType()

	// add User type
	reg.Register(User)

	// make sure it contains the User type
	assert.Equal(t, User, reg.content[User.Hash])

	// test contains function
	assert.Equal(t, true, reg.Contains(User))

	// test contains hash function
	assert.Equal(t, true, reg.ContainsHash(User.Hash))

	// test contains name function
	assert.Equal(t, true, reg.ContainsName(User.Name))
}

func TestRegistryContainsFalse(t *testing.T) {

	// create new empty registry
	reg := NewRegistry()

	// create type
	User := createTestTupleType()

	// DO NOT add User type
	// reg.Register(User)

	// make sure it DOES NOT contains the User type
	assert.Equal(t, TupleType{}, reg.content[User.Hash])

	// test contains function
	assert.Equal(t, false, reg.Contains(User))

	// test contains hash function
	assert.Equal(t, false, reg.ContainsHash(User.Hash))

	// test contains name function
	assert.Equal(t, false, reg.ContainsName(User.Name))
}

func TestRegistryGetTrue(t *testing.T) {

	// create new empty registry
	reg := NewRegistry()

	// create type
	User := createTestTupleType()

	// add User type
	reg.Register(User)

	// make sure the registry contains the same User type
	tupleType, exists := reg.Get(User.Hash)
	assert.Equal(t, User, tupleType)
	assert.Equal(t, true, exists)
}

func TestRegistryGetFalse(t *testing.T) {

	// create new empty registry
	reg := NewRegistry()

	// create type
	User := createTestTupleType()

	// DO NOT add User type
	// reg.Register(User)

	// make sure the registry contains the same User type
	tupleType, exists := reg.Get(User.Hash)
	assert.Equal(t, TupleType{}, tupleType)
	assert.Equal(t, false, exists)
}
