// Package store implements the deduplicated storage engine for KubeClient.
// This file contains the concrete DedupStore implementation that uses value
// interning and subtree deduplication to minimise memory usage.
package store

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// DedupStore is the main deduplicated storage engine.
// It decomposes Unstructured objects into a content-addressable node tree,
// deduplicating identical values and subtrees across all stored objects.
type DedupStore struct {
	mu       sync.RWMutex
	values   *ValueInternPool
	subtrees *SubtreeInternPool

	// objects maps each ObjectKey to its root NodeID.
	objects map[ObjectKey]NodeID

	// refcounts tracks how many object roots reference each NodeID (transitively).
	// When refcount reaches 0, the node can be reclaimed by GC.
	refcounts map[NodeID]uint32
}

// NewDedupStore creates a new DedupStore.
func NewDedupStore() *DedupStore {
	return &DedupStore{
		values:    NewValueInternPool(),
		subtrees:  NewSubtreeInternPool(),
		objects:   make(map[ObjectKey]NodeID),
		refcounts: make(map[NodeID]uint32),
	}
}

// Upsert adds or updates an object. The object is decomposed into the node tree.
func (s *DedupStore) Upsert(key ObjectKey, obj *unstructured.Unstructured) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	newRoot := s.decompose(obj.Object)

	// If the key already exists, decrement refcounts for the old subtree.
	if oldRoot, ok := s.objects[key]; ok {
		s.decRef(oldRoot)
	}

	s.objects[key] = newRoot
	s.incRef(newRoot)
	return nil
}

// Delete removes an object and decrements refcounts for its subtree.
func (s *DedupStore) Delete(key ObjectKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	root, ok := s.objects[key]
	if !ok {
		return nil
	}

	s.decRef(root)
	delete(s.objects, key)
	return nil
}

// Get retrieves an object, reconstructing it from the node tree.
func (s *DedupStore) Get(key ObjectKey) (*unstructured.Unstructured, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	root, ok := s.objects[key]
	if !ok {
		return nil, false
	}

	raw := s.reconstruct(root)
	m, ok := raw.(map[string]interface{})
	if !ok {
		return nil, false
	}
	return &unstructured.Unstructured{Object: m}, true
}

// List returns all objects for the given GVK, applying any options.
func (s *DedupStore) List(gvk schema.GroupVersionKind, opts ...ListOption) []*unstructured.Unstructured {
	lo := &listOptions{}
	for _, opt := range opts {
		opt(lo)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*unstructured.Unstructured
	for key, root := range s.objects {
		if key.GVK != gvk {
			continue
		}
		if lo.namespace != "" && key.Namespace != lo.namespace {
			continue
		}

		raw := s.reconstruct(root)
		m, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		obj := &unstructured.Unstructured{Object: m}

		if len(lo.labelSelector) > 0 {
			objLabels := obj.GetLabels()
			if !labelsMatch(objLabels, lo.labelSelector) {
				continue
			}
		}

		if len(lo.fieldSelectors) > 0 {
			match := true
			for field, value := range lo.fieldSelectors {
				if !fieldMatches(obj, field, value) {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		result = append(result, obj)
	}
	return result
}

// GC removes unreachable nodes and values from the pools.
// It walks all live object roots to find reachable NodeIDs and ValueIDs,
// then removes everything else from the subtrees and values pools.
func (s *DedupStore) GC() GCStats {
	start := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Step 1: remove zero-refcount entries from the refcounts map.
	for id, count := range s.refcounts {
		if count == 0 {
			delete(s.refcounts, id)
		}
	}

	// Step 2: collect all live root NodeIDs from the objects map.
	liveNodes := make(map[NodeID]struct{})
	liveValues := make(map[ValueID]struct{})

	// Walk the subtree graph from each live root to find all reachable NodeIDs and ValueIDs.
	var walk func(id NodeID)
	walk = func(id NodeID) {
		if id == 0 {
			return
		}
		if _, seen := liveNodes[id]; seen {
			return
		}
		liveNodes[id] = struct{}{}
		node := s.subtrees.Resolve(id)
		switch node.Kind {
		case NodeKindScalar:
			liveValues[node.ValueID] = struct{}{}
		case NodeKindMap:
			for _, entry := range node.MapEntries {
				liveValues[entry.Key] = struct{}{}
				walk(entry.Value)
			}
		case NodeKindSlice:
			for _, item := range node.SliceItems {
				walk(item)
			}
		}
	}

	for _, rootID := range s.objects {
		walk(rootID)
	}

	// Step 3: remove unreachable NodeIDs from the subtrees pool.
	nodesRemoved := s.subtrees.DeleteUnreachable(liveNodes)

	// Step 4: remove unreachable ValueIDs from the values pool.
	valuesRemoved := s.values.DeleteUnreachable(liveValues)

	return GCStats{
		NodesRemoved:  nodesRemoved,
		ValuesRemoved: valuesRemoved,
		Duration:      time.Since(start),
	}
}

// decompose recursively converts an interface{} value into a NodeID.
// It interns all values and subtrees. Must be called with s.mu held (write).
func (s *DedupStore) decompose(v interface{}) NodeID {
	switch t := v.(type) {
	case nil:
		return s.subtrees.Intern(Node{
			Kind:    NodeKindScalar,
			ValueID: 0, // nil sentinel
		})

	case string:
		vid := s.values.Intern(t)
		return s.subtrees.Intern(Node{Kind: NodeKindScalar, ValueID: vid})

	case int64:
		vid := s.values.Intern(t)
		return s.subtrees.Intern(Node{Kind: NodeKindScalar, ValueID: vid})

	case float64:
		vid := s.values.Intern(t)
		return s.subtrees.Intern(Node{Kind: NodeKindScalar, ValueID: vid})

	case bool:
		vid := s.values.Intern(t)
		return s.subtrees.Intern(Node{Kind: NodeKindScalar, ValueID: vid})

	case int:
		vid := s.values.Intern(int64(t))
		return s.subtrees.Intern(Node{Kind: NodeKindScalar, ValueID: vid})

	case int32:
		vid := s.values.Intern(int64(t))
		return s.subtrees.Intern(Node{Kind: NodeKindScalar, ValueID: vid})

	case uint:
		vid := s.values.Intern(int64(t))
		return s.subtrees.Intern(Node{Kind: NodeKindScalar, ValueID: vid})

	case uint32:
		vid := s.values.Intern(int64(t))
		return s.subtrees.Intern(Node{Kind: NodeKindScalar, ValueID: vid})

	case uint64:
		vid := s.values.Intern(int64(t))
		return s.subtrees.Intern(Node{Kind: NodeKindScalar, ValueID: vid})

	case map[string]interface{}:
		entries := make([]MapEntry, 0, len(t))
		for k, val := range t {
			keyID := s.values.Intern(k)
			valID := s.decompose(val)
			entries = append(entries, MapEntry{Key: keyID, Value: valID})
		}
		// Sort by Key (ValueID) for deterministic hashing.
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Key < entries[j].Key
		})
		return s.subtrees.Intern(Node{Kind: NodeKindMap, MapEntries: entries})

	case []interface{}:
		items := make([]NodeID, len(t))
		for i, elem := range t {
			items[i] = s.decompose(elem)
		}
		return s.subtrees.Intern(Node{Kind: NodeKindSlice, SliceItems: items})

	default:
		vid := s.values.Intern(fmt.Sprintf("%v", t))
		return s.subtrees.Intern(Node{Kind: NodeKindScalar, ValueID: vid})
	}
}

// reconstruct recursively rebuilds an interface{} from a NodeID.
// Must be called with s.mu held (at least read).
func (s *DedupStore) reconstruct(id NodeID) interface{} {
	node := s.subtrees.Resolve(id)

	switch node.Kind {
	case NodeKindScalar:
		return s.values.Resolve(node.ValueID)

	case NodeKindMap:
		m := make(map[string]interface{}, len(node.MapEntries))
		for _, entry := range node.MapEntries {
			key, _ := s.values.Resolve(entry.Key).(string)
			m[key] = s.reconstruct(entry.Value)
		}
		return m

	case NodeKindSlice:
		sl := make([]interface{}, len(node.SliceItems))
		for i, item := range node.SliceItems {
			sl[i] = s.reconstruct(item)
		}
		return sl
	}

	return nil
}

// incRef increments refcounts for all nodes reachable from the given NodeID.
// Must be called with s.mu held (write).
func (s *DedupStore) incRef(id NodeID) {
	if id == 0 {
		return
	}
	s.refcounts[id]++
	node := s.subtrees.Resolve(id)
	switch node.Kind {
	case NodeKindMap:
		for _, entry := range node.MapEntries {
			s.incRef(entry.Value)
		}
	case NodeKindSlice:
		for _, item := range node.SliceItems {
			s.incRef(item)
		}
	}
}

// decRef decrements refcounts for all nodes reachable from the given NodeID.
// Must be called with s.mu held (write).
func (s *DedupStore) decRef(id NodeID) {
	if id == 0 {
		return
	}
	if s.refcounts[id] > 0 {
		s.refcounts[id]--
	}
	node := s.subtrees.Resolve(id)
	switch node.Kind {
	case NodeKindMap:
		for _, entry := range node.MapEntries {
			s.decRef(entry.Value)
		}
	case NodeKindSlice:
		for _, item := range node.SliceItems {
			s.decRef(item)
		}
	}
}

// labelsMatch returns true if all selector labels are present and equal in objLabels.
func labelsMatch(objLabels, selector map[string]string) bool {
	for k, v := range selector {
		if objLabels[k] != v {
			return false
		}
	}
	return true
}

// fieldMatches returns true if the dot-notation field path resolves to the given value
// in the unstructured object. For example, field="spec.nodeName" traverses
// obj.Object["spec"]["nodeName"] and compares it to value as a string.
func fieldMatches(obj *unstructured.Unstructured, field, value string) bool {
	parts := strings.Split(field, ".")
	var current interface{} = obj.Object
	for _, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return false
		}
		current, ok = m[part]
		if !ok {
			return false
		}
	}
	// Compare as string representation.
	switch v := current.(type) {
	case string:
		return v == value
	default:
		return fmt.Sprintf("%v", v) == value
	}
}
