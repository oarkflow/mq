package storage

// IMap is a thread-safe map interface.
type IMap[K comparable, V any] interface {
	Get(K) (V, bool)
	Set(K, V)
	Del(K)
	ForEach(func(K, V) bool)
	Clear()
	Size() int
	Keys() []K
	Values() []V
	AsMap() map[K]V
	Clone() IMap[K, V]
}
