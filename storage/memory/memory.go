package memory

import (
	"sync"

	"github.com/oarkflow/mq/storage"
)

var _ storage.IMap[string, any] = (*Map[string, any])(nil)

type Map[K comparable, V any] struct {
	data map[K]V
	mu   sync.RWMutex
}

func New[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{
		data: make(map[K]V),
	}
}

func (m *Map[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, exists := m.data[key]
	return val, exists
}

func (m *Map[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

func (m *Map[K, V]) Del(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

func (m *Map[K, V]) ForEach(f func(K, V) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.data {
		if !f(k, v) {
			break
		}
	}
}

func (m *Map[K, V]) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[K]V)
}

func (m *Map[K, V]) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

func (m *Map[K, V]) Keys() []K {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]K, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

func (m *Map[K, V]) Values() []V {
	m.mu.RLock()
	defer m.mu.RUnlock()
	values := make([]V, 0, len(m.data))
	for _, v := range m.data {
		values = append(values, v)
	}
	return values
}

func (m *Map[K, V]) AsMap() map[K]V {
	m.mu.RLock()
	defer m.mu.RUnlock()
	copiedMap := make(map[K]V, len(m.data))
	for k, v := range m.data {
		copiedMap[k] = v
	}
	return copiedMap
}

func (m *Map[K, V]) Clone() storage.IMap[K, V] {
	m.mu.RLock()
	defer m.mu.RUnlock()
	clonedMap := New[K, V]()
	for k, v := range m.data {
		clonedMap.Set(k, v)
	}
	return clonedMap
}
