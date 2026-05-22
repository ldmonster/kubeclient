// Package store implements the deduplicated storage engine for KubeClient.
// This file implements the SubtreeInternPool, which hashes and deduplicates
// entire map/slice subtrees so identical structures are stored only once.
package store

import (
	"encoding/binary"
	"hash/fnv"
	"sync"
)

// SubtreeInternPool stores unique Node subtrees.
// Identical nodes (same Kind, same content) always get the same NodeID.
// NodeID 0 is reserved as the "null/empty" sentinel.
// Thread-safe for concurrent use.
type SubtreeInternPool struct {
	mu    sync.RWMutex
	nodes []Node              // NodeID -> Node (index 0 = sentinel)
	index map[uint64][]NodeID // hash -> candidate NodeIDs (collision handling)
}

// NewSubtreeInternPool creates a new SubtreeInternPool.
func NewSubtreeInternPool() *SubtreeInternPool {
	return &SubtreeInternPool{
		// index 0 is the sentinel (zero-value Node)
		nodes: []Node{{}},
		index: make(map[uint64][]NodeID),
	}
}

// Intern stores a node and returns its NodeID.
// If an identical node already exists, returns the existing NodeID.
func (p *SubtreeInternPool) Intern(node Node) NodeID {
	h := p.hash(node)

	// Fast path: read lock.
	p.mu.RLock()
	if candidates, ok := p.index[h]; ok {
		for _, id := range candidates {
			if p.equal(p.nodes[id], node) {
				p.mu.RUnlock()
				return id
			}
		}
	}
	p.mu.RUnlock()

	// Slow path: write lock.
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock.
	if candidates, ok := p.index[h]; ok {
		for _, id := range candidates {
			if p.equal(p.nodes[id], node) {
				return id
			}
		}
	}

	id := NodeID(len(p.nodes))
	p.nodes = append(p.nodes, node)
	p.index[h] = append(p.index[h], id)
	return id
}

// Resolve returns the Node for a given NodeID.
func (p *SubtreeInternPool) Resolve(id NodeID) Node {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if int(id) >= len(p.nodes) {
		return Node{}
	}
	return p.nodes[id]
}

// Len returns the number of interned nodes (including the sentinel at index 0).
func (p *SubtreeInternPool) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.nodes)
}

// DeleteUnreachable removes all nodes whose NodeID is not present in the live set.
// NodeID 0 (the sentinel) is always preserved.
// Returns the number of nodes removed.
func (p *SubtreeInternPool) DeleteUnreachable(live map[NodeID]struct{}) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	removed := 0
	// Rebuild the index, dropping entries for unreachable nodes.
	newIndex := make(map[uint64][]NodeID, len(p.index))
	for h, candidates := range p.index {
		var kept []NodeID
		for _, id := range candidates {
			if id == 0 {
				kept = append(kept, id)
				continue
			}
			if _, ok := live[id]; ok {
				kept = append(kept, id)
			} else {
				// Zero out the node slot to free memory; mark as removed.
				p.nodes[id] = Node{}
				removed++
			}
		}
		if len(kept) > 0 {
			newIndex[h] = kept
		}
	}
	p.index = newIndex
	return removed
}

// hash computes a deterministic FNV-1a 64-bit hash of a Node.
// Since child NodeIDs are already content-addressable, the hash is stable.
func (p *SubtreeInternPool) hash(node Node) uint64 {
	h := fnv.New64a()
	buf := make([]byte, 8)

	// Mix in the node kind so different kinds with the same payload don't collide.
	h.Write([]byte{byte(node.Kind)})

	switch node.Kind {
	case NodeKindScalar:
		binary.LittleEndian.PutUint64(buf, uint64(node.ValueID))
		h.Write(buf)

	case NodeKindMap:
		for _, entry := range node.MapEntries {
			binary.LittleEndian.PutUint64(buf, uint64(entry.Key))
			h.Write(buf)
			binary.LittleEndian.PutUint64(buf, uint64(entry.Value))
			h.Write(buf)
		}

	case NodeKindSlice:
		for _, item := range node.SliceItems {
			binary.LittleEndian.PutUint64(buf, uint64(item))
			h.Write(buf)
		}
	}

	return h.Sum64()
}

// equal checks if two nodes are structurally identical.
func (p *SubtreeInternPool) equal(a, b Node) bool {
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case NodeKindScalar:
		return a.ValueID == b.ValueID

	case NodeKindMap:
		if len(a.MapEntries) != len(b.MapEntries) {
			return false
		}
		for i := range a.MapEntries {
			if a.MapEntries[i].Key != b.MapEntries[i].Key ||
				a.MapEntries[i].Value != b.MapEntries[i].Value {
				return false
			}
		}
		return true

	case NodeKindSlice:
		if len(a.SliceItems) != len(b.SliceItems) {
			return false
		}
		for i := range a.SliceItems {
			if a.SliceItems[i] != b.SliceItems[i] {
				return false
			}
		}
		return true
	}
	return false
}
