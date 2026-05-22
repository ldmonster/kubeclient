// Package store implements the deduplicated storage engine for KubeClient.
// This file defines the internal Node representation used to decompose
// Kubernetes Unstructured objects into a deduplicated tree of values.
package store

// NodeID is a unique identifier for a node in the SubtreeInternPool.
type NodeID uint64

// ValueID is a unique identifier for a value in the ValueInternPool.
type ValueID uint64

// NodeKind represents the type of a node.
type NodeKind byte

const (
	NodeKindScalar NodeKind = iota // string, int64, float64, bool, nil
	NodeKindMap                    // map[string]interface{}
	NodeKindSlice                  // []interface{}
)

// MapEntry is a key-value pair in a map node.
type MapEntry struct {
	Key   ValueID // interned string key
	Value NodeID  // child node
}

// Node represents a single node in the decomposed object tree.
type Node struct {
	Kind       NodeKind
	ValueID    ValueID    // only for NodeKindScalar
	MapEntries []MapEntry // only for NodeKindMap, sorted by Key
	SliceItems []NodeID   // only for NodeKindSlice
}
