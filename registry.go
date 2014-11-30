package namedtuple

import (
	"hash/fnv"
	"sync"
)

func NewRegistry() Registry {
	return Registry{content: make(map[uint32]TupleType), hasher: NewHasher(fnv.New32a())}
}

type Registry struct {
	content map[uint32]TupleType
	hasher  SynchronizedHash
	mutex   sync.Mutex
}

func (r *Registry) Contains(t TupleType) bool {
	return r.ContainsHash(t.Hash)
}

func (r *Registry) ContainsHash(hash uint32) bool {
	// lock registry
	r.mutex.Lock()
	defer r.mutex.Unlock()

	_, exists := r.content[hash]
	return exists
}

func (r *Registry) ContainsName(name string) bool {
	return r.ContainsHash(r.hasher.Hash([]byte(name)))
}

func (r *Registry) Get(hash uint32) (tupleType TupleType, exists bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	tupleType, exists = r.content[hash]
	return
}

func (r *Registry) Register(t TupleType) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.content[t.Hash]; !exists {
		r.content[t.Hash] = t
	}
}

func (r *Registry) Unregister(t TupleType) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.content[t.Hash]; exists {
		delete(r.content, t.Hash)
	}
}

func (r *Registry) Size() int {
	return len(r.content)
}
