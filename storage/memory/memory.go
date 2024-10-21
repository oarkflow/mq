package memory

import (
	"sync"

	"github.com/oarkflow/mq/storage"
)

var _ storage.IMap[string, any] = (*Map[string, any])(nil)

// Map is a thread-safe map using sync.Map with generics
type Map[K comparable, V any] struct {
	m sync.Map
}

// New creates a new Map
func New[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{}
}

// Get retrieves the value for a given key
func (g *Map[K, V]) Get(key K) (V, bool) {
	val, ok := g.m.Load(key)
	if !ok {
		var zeroValue V
		return zeroValue, false
	}
	return val.(V), true
}

// Set adds a key-value pair to the map
func (g *Map[K, V]) Set(key K, value V) {
	g.m.Store(key, value)
}

// Del removes a key-value pair from the map
func (g *Map[K, V]) Del(key K) {
	g.m.Delete(key)
}

// ForEach iterates over the map
func (g *Map[K, V]) ForEach(fn func(K, V) bool) {
	g.m.Range(func(k, v any) bool {
		return fn(k.(K), v.(V))
	})
}

// Clear removes all key-value pairs from the map
func (g *Map[K, V]) Clear() {
	g.ForEach(func(k K, v V) bool {
		g.Del(k)
		return true
	})
}

// Size returns the number of key-value pairs in the map
func (g *Map[K, V]) Size() int {
	count := 0
	g.ForEach(func(_ K, _ V) bool {
		count++
		return true
	})
	return count
}

// Keys returns a slice of all keys in the map
func (g *Map[K, V]) Keys() []K {
	var keys []K
	g.ForEach(func(k K, _ V) bool {
		keys = append(keys, k)
		return true
	})
	return keys
}

// Values returns a slice of all values in the map
func (g *Map[K, V]) Values() []V {
	var values []V
	g.ForEach(func(_ K, v V) bool {
		values = append(values, v)
		return true
	})
	return values
}

// AsMap returns a regular map containing all key-value pairs
func (g *Map[K, V]) AsMap() map[K]V {
	result := make(map[K]V)
	g.ForEach(func(k K, v V) bool {
		result[k] = v
		return true
	})
	return result
}

// Clone creates a shallow copy of the map
func (g *Map[K, V]) Clone() storage.IMap[K, V] {
	clone := New[K, V]()
	g.ForEach(func(k K, v V) bool {
		clone.Set(k, v)
		return true
	})
	return clone
}
