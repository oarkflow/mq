package dag

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// RingBuffer is a lock-free, high-throughput ring buffer implementation
// using atomic operations for concurrent access without locks.
type RingBuffer[T any] struct {
	buffer   []unsafe.Pointer
	capacity uint64
	mask     uint64
	head     uint64   // Write position (producer)
	tail     uint64   // Read position (consumer)
	_padding [64]byte // Cache line padding to prevent false sharing
}

// NewRingBuffer creates a new lock-free ring buffer with the specified capacity.
// Capacity must be a power of 2 for optimal performance using bitwise operations.
func NewRingBuffer[T any](capacity uint64) *RingBuffer[T] {
	// Round up to next power of 2 if not already
	if capacity == 0 {
		capacity = 1024
	}
	if capacity&(capacity-1) != 0 {
		// Not a power of 2, round up
		capacity = nextPowerOf2(capacity)
	}

	return &RingBuffer[T]{
		buffer:   make([]unsafe.Pointer, capacity),
		capacity: capacity,
		mask:     capacity - 1, // For fast modulo using bitwise AND
		head:     0,
		tail:     0,
	}
}

// Push adds an item to the ring buffer.
// Returns true if successful, false if buffer is full.
// This is a lock-free operation using atomic CAS (Compare-And-Swap).
func (rb *RingBuffer[T]) Push(item T) bool {
	for {
		head := atomic.LoadUint64(&rb.head)
		tail := atomic.LoadUint64(&rb.tail)

		// Check if buffer is full
		if head-tail >= rb.capacity {
			return false
		}

		// Try to claim this slot
		if atomic.CompareAndSwapUint64(&rb.head, head, head+1) {
			// Successfully claimed slot, now write the item
			idx := head & rb.mask
			atomic.StorePointer(&rb.buffer[idx], unsafe.Pointer(&item))
			return true
		}
		// CAS failed, retry
		runtime.Gosched() // Yield to other goroutines
	}
}

// Pop removes and returns an item from the ring buffer.
// Returns the item and true if successful, zero value and false if buffer is empty.
// This is a lock-free operation using atomic CAS.
func (rb *RingBuffer[T]) Pop() (T, bool) {
	var zero T
	for {
		tail := atomic.LoadUint64(&rb.tail)
		head := atomic.LoadUint64(&rb.head)

		// Check if buffer is empty
		if tail >= head {
			return zero, false
		}

		// Try to claim this slot
		if atomic.CompareAndSwapUint64(&rb.tail, tail, tail+1) {
			// Successfully claimed slot, now read the item
			idx := tail & rb.mask
			ptr := atomic.LoadPointer(&rb.buffer[idx])
			if ptr == nil {
				// Item not yet written, retry
				continue
			}
			item := *(*T)(ptr)
			// Clear the slot to allow GC
			atomic.StorePointer(&rb.buffer[idx], nil)
			return item, true
		}
		// CAS failed, retry
		runtime.Gosched()
	}
}

// TryPush attempts to push an item without retrying.
// Returns true if successful, false if buffer is full or contention.
func (rb *RingBuffer[T]) TryPush(item T) bool {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)

	// Check if buffer is full
	if head-tail >= rb.capacity {
		return false
	}

	// Try to claim this slot (single attempt)
	if atomic.CompareAndSwapUint64(&rb.head, head, head+1) {
		idx := head & rb.mask
		atomic.StorePointer(&rb.buffer[idx], unsafe.Pointer(&item))
		return true
	}
	return false
}

// TryPop attempts to pop an item without retrying.
// Returns the item and true if successful, zero value and false if empty or contention.
func (rb *RingBuffer[T]) TryPop() (T, bool) {
	var zero T
	tail := atomic.LoadUint64(&rb.tail)
	head := atomic.LoadUint64(&rb.head)

	// Check if buffer is empty
	if tail >= head {
		return zero, false
	}

	// Try to claim this slot (single attempt)
	if atomic.CompareAndSwapUint64(&rb.tail, tail, tail+1) {
		idx := tail & rb.mask
		ptr := atomic.LoadPointer(&rb.buffer[idx])
		if ptr == nil {
			return zero, false
		}
		item := *(*T)(ptr)
		atomic.StorePointer(&rb.buffer[idx], nil)
		return item, true
	}
	return zero, false
}

// Size returns the current number of items in the buffer.
// Note: This is an approximate value due to concurrent access.
func (rb *RingBuffer[T]) Size() uint64 {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
	if head >= tail {
		return head - tail
	}
	return 0
}

// Capacity returns the maximum capacity of the buffer.
func (rb *RingBuffer[T]) Capacity() uint64 {
	return rb.capacity
}

// IsEmpty returns true if the buffer is empty.
func (rb *RingBuffer[T]) IsEmpty() bool {
	return rb.Size() == 0
}

// IsFull returns true if the buffer is full.
func (rb *RingBuffer[T]) IsFull() bool {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
	return head-tail >= rb.capacity
}

// Reset clears all items from the buffer.
// WARNING: Not thread-safe, should only be called when no other goroutines are accessing the buffer.
func (rb *RingBuffer[T]) Reset() {
	atomic.StoreUint64(&rb.head, 0)
	atomic.StoreUint64(&rb.tail, 0)
	for i := range rb.buffer {
		atomic.StorePointer(&rb.buffer[i], nil)
	}
}

// nextPowerOf2 returns the next power of 2 greater than or equal to n.
func nextPowerOf2(n uint64) uint64 {
	if n == 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}

// StreamingRingBuffer is a specialized ring buffer optimized for linear streaming
// with batch operations for higher throughput.
type StreamingRingBuffer[T any] struct {
	buffer    []unsafe.Pointer
	capacity  uint64
	mask      uint64
	head      uint64
	tail      uint64
	batchSize uint64
	_padding  [64]byte
}

// NewStreamingRingBuffer creates a new streaming ring buffer optimized for batch operations.
func NewStreamingRingBuffer[T any](capacity, batchSize uint64) *StreamingRingBuffer[T] {
	if capacity == 0 {
		capacity = 4096
	}
	if capacity&(capacity-1) != 0 {
		capacity = nextPowerOf2(capacity)
	}
	if batchSize == 0 {
		batchSize = 64
	}

	return &StreamingRingBuffer[T]{
		buffer:    make([]unsafe.Pointer, capacity),
		capacity:  capacity,
		mask:      capacity - 1,
		head:      0,
		tail:      0,
		batchSize: batchSize,
	}
}

// PushBatch pushes multiple items in a batch for better throughput.
// Returns the number of items successfully pushed.
func (srb *StreamingRingBuffer[T]) PushBatch(items []T) int {
	if len(items) == 0 {
		return 0
	}

	pushed := 0
	for pushed < len(items) {
		head := atomic.LoadUint64(&srb.head)
		tail := atomic.LoadUint64(&srb.tail)

		available := srb.capacity - (head - tail)
		if available == 0 {
			break
		}

		// Push as many items as possible in this batch
		batchEnd := pushed + int(available)
		if batchEnd > len(items) {
			batchEnd = len(items)
		}

		// Try to claim slots for this batch
		batchCount := uint64(batchEnd - pushed)
		if atomic.CompareAndSwapUint64(&srb.head, head, head+batchCount) {
			// Successfully claimed slots, write items
			for i := pushed; i < batchEnd; i++ {
				idx := (head + uint64(i-pushed)) & srb.mask
				atomic.StorePointer(&srb.buffer[idx], unsafe.Pointer(&items[i]))
			}
			pushed = batchEnd
		} else {
			runtime.Gosched()
		}
	}
	return pushed
}

// PopBatch pops multiple items in a batch for better throughput.
// Returns a slice of items successfully popped.
func (srb *StreamingRingBuffer[T]) PopBatch(maxItems int) []T {
	if maxItems <= 0 {
		return nil
	}

	result := make([]T, 0, maxItems)

	for len(result) < maxItems {
		tail := atomic.LoadUint64(&srb.tail)
		head := atomic.LoadUint64(&srb.head)

		available := head - tail
		if available == 0 {
			break
		}

		// Pop as many items as possible in this batch
		batchSize := uint64(maxItems - len(result))
		if batchSize > available {
			batchSize = available
		}
		if batchSize > srb.batchSize {
			batchSize = srb.batchSize
		}

		// Try to claim slots for this batch
		if atomic.CompareAndSwapUint64(&srb.tail, tail, tail+batchSize) {
			// Successfully claimed slots, read items
			for i := uint64(0); i < batchSize; i++ {
				idx := (tail + i) & srb.mask
				ptr := atomic.LoadPointer(&srb.buffer[idx])
				if ptr != nil {
					item := *(*T)(ptr)
					result = append(result, item)
					atomic.StorePointer(&srb.buffer[idx], nil)
				}
			}
		} else {
			runtime.Gosched()
		}
	}
	return result
}

// Push adds a single item to the streaming buffer.
func (srb *StreamingRingBuffer[T]) Push(item T) bool {
	for {
		head := atomic.LoadUint64(&srb.head)
		tail := atomic.LoadUint64(&srb.tail)

		if head-tail >= srb.capacity {
			return false
		}

		if atomic.CompareAndSwapUint64(&srb.head, head, head+1) {
			idx := head & srb.mask
			atomic.StorePointer(&srb.buffer[idx], unsafe.Pointer(&item))
			return true
		}
		runtime.Gosched()
	}
}

// Pop removes a single item from the streaming buffer.
func (srb *StreamingRingBuffer[T]) Pop() (T, bool) {
	var zero T
	for {
		tail := atomic.LoadUint64(&srb.tail)
		head := atomic.LoadUint64(&srb.head)

		if tail >= head {
			return zero, false
		}

		if atomic.CompareAndSwapUint64(&srb.tail, tail, tail+1) {
			idx := tail & srb.mask
			ptr := atomic.LoadPointer(&srb.buffer[idx])
			if ptr == nil {
				continue
			}
			item := *(*T)(ptr)
			atomic.StorePointer(&srb.buffer[idx], nil)
			return item, true
		}
		runtime.Gosched()
	}
}

// Size returns the approximate number of items in the buffer.
func (srb *StreamingRingBuffer[T]) Size() uint64 {
	head := atomic.LoadUint64(&srb.head)
	tail := atomic.LoadUint64(&srb.tail)
	if head >= tail {
		return head - tail
	}
	return 0
}

// Capacity returns the maximum capacity of the buffer.
func (srb *StreamingRingBuffer[T]) Capacity() uint64 {
	return srb.capacity
}
