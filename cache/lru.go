// Package cache implements the caching layer for KubeClient.
// This file implements a simple thread-safe fixed-size LRU cache using only
// the standard library (container/list for the doubly-linked list).
package cache

import (
	"container/list"
	"sync"
)

// lruEntry is the value stored in the LRU map.
type lruEntry struct {
	key string
	val interface{}
	// elem is the corresponding element in the doubly-linked list.
	elem *list.Element
}

// lruCache is a thread-safe fixed-capacity LRU cache.
// The key is a string (e.g. "group/version/kind/namespace/name").
// The value is an arbitrary interface{} (typically *unstructured.Unstructured).
type lruCache struct { //nolint:unused
	mu       sync.Mutex
	capacity int
	items    map[string]*lruEntry
	order    *list.List // front = most-recently-used, back = least-recently-used
}

// newLRUCache creates a new lruCache with the given capacity.
// capacity must be > 0.
func newLRUCache(capacity int) *lruCache {
	if capacity <= 0 {
		capacity = 1
	}
	return &lruCache{
		capacity: capacity,
		items:    make(map[string]*lruEntry, capacity),
		order:    list.New(),
	}
}

// get returns the cached value for key and true, or nil and false on a miss.
// On a hit the entry is promoted to the front (most-recently-used).
func (c *lruCache) get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.items[key]
	if !ok {
		return nil, false
	}
	// Promote to front.
	c.order.MoveToFront(entry.elem)
	return entry.val, true
}

// put inserts or updates the value for key.
// If the cache is at capacity the least-recently-used entry is evicted first.
func (c *lruCache) put(key string, val interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update existing entry.
	if entry, ok := c.items[key]; ok {
		entry.val = val
		c.order.MoveToFront(entry.elem)
		return
	}

	// Evict LRU entry if at capacity.
	if len(c.items) >= c.capacity {
		back := c.order.Back()
		if back != nil {
			evicted := back.Value.(*lruEntry)
			c.order.Remove(back)
			delete(c.items, evicted.key)
		}
	}

	// Insert new entry at front.
	entry := &lruEntry{key: key, val: val}
	elem := c.order.PushFront(entry)
	entry.elem = elem
	c.items[key] = entry
}

// delete removes the entry for key from the cache (no-op if absent).
func (c *lruCache) delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.items[key]
	if !ok {
		return
	}
	c.order.Remove(entry.elem)
	delete(c.items, key)
}

// len returns the current number of entries in the cache.
func (c *lruCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// LRUCache is a thread-safe fixed-capacity LRU cache (exported for per-client use).
type LRUCache = lruCache

// NewLRUCache creates a new LRUCache with the given capacity.
func NewLRUCache(capacity int) *LRUCache {
	return newLRUCache(capacity)
}

// Delete removes the entry for key from the cache (exported).
func (c *lruCache) Delete(key string) {
	c.delete(key)
}

// Get returns the cached value for key and true, or nil and false on a miss (exported).
func (c *lruCache) Get(key string) (interface{}, bool) {
	return c.get(key)
}
