// Package store implements the deduplicated storage engine for KubeClient.
// This file implements the ValueInternPool, which stores unique scalar values
// (strings, int64, float64, bool, nil) and returns compact ValueIDs.
package store

import (
	"fmt"
	"sync"
)

// ValueInternPool stores unique scalar values and returns compact ValueIDs.
// Identical values always map to the same ValueID.
// Thread-safe for concurrent use.
type ValueInternPool struct {
	mu     sync.RWMutex
	values []interface{}           // ValueID -> value (index 0 = nil sentinel)
	index  map[interface{}]ValueID // value -> ValueID (nil not stored here)
}

// NewValueInternPool creates a new ValueInternPool.
// ValueID 0 is reserved for nil.
func NewValueInternPool() *ValueInternPool {
	return &ValueInternPool{
		// index 0 is the nil sentinel; pre-allocate with a nil placeholder
		values: []interface{}{nil},
		index:  make(map[interface{}]ValueID),
	}
}

// Intern returns the ValueID for the given value, creating a new entry if needed.
// Supported types: nil, string, int64, float64, bool.
// Other types are converted to string via fmt.Sprintf.
func (p *ValueInternPool) Intern(v interface{}) ValueID {
	// nil is always ValueID 0 — no map lookup needed.
	if v == nil {
		return 0
	}

	// Normalise numeric types that are not directly supported.
	switch t := v.(type) {
	case int:
		v = int64(t)
	case int32:
		v = int64(t)
	case uint:
		v = int64(t)
	case uint32:
		v = int64(t)
	case uint64:
		v = int64(t)
	case string, int64, float64, bool:
		// already canonical
	default:
		v = fmt.Sprintf("%v", t)
	}

	// Fast path: read lock.
	p.mu.RLock()
	if id, ok := p.index[v]; ok {
		p.mu.RUnlock()
		return id
	}
	p.mu.RUnlock()

	// Slow path: write lock.
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock.
	if id, ok := p.index[v]; ok {
		return id
	}

	id := ValueID(len(p.values))
	p.values = append(p.values, v)
	p.index[v] = id
	return id
}

// Resolve returns the original value for a ValueID.
func (p *ValueInternPool) Resolve(id ValueID) interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if int(id) >= len(p.values) {
		return nil
	}
	return p.values[id]
}

// Len returns the number of interned values (including the nil sentinel at index 0).
func (p *ValueInternPool) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.values)
}

// DeleteUnreachable removes all values whose ValueID is not present in the live set.
// ValueID 0 (the nil sentinel) is always preserved.
// Returns the number of values removed.
func (p *ValueInternPool) DeleteUnreachable(live map[ValueID]struct{}) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	removed := 0
	// p.index maps value (interface{}) -> ValueID.
	for val, vid := range p.index {
		if vid == 0 {
			continue
		}
		if _, ok := live[vid]; !ok {
			delete(p.index, val)
			p.values[vid] = nil
			removed++
		}
	}
	return removed
}
