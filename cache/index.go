// Package cache implements the caching layer for KubeClient.
// This file implements the indexing subsystem for efficient field-based lookups.
package cache

import (
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ldmonster/kubeclient/store"
)

// IndexerFunc extracts index values from an object.
// Returns a list of index values for the object (can be multiple).
type IndexerFunc func(obj client.Object) []string

// Indexer manages field indexes for cached objects.
// It allows efficient lookups by arbitrary fields.
// Thread-safe for concurrent use.
type Indexer struct {
	mu sync.RWMutex

	// funcs: GVK -> fieldName -> IndexerFunc
	funcs map[schema.GroupVersionKind]map[string]IndexerFunc

	// data: GVK -> fieldName -> indexValue -> set of ObjectKeys
	data map[schema.GroupVersionKind]map[string]map[string]map[store.ObjectKey]struct{}

	// reverse: ObjectKey -> fieldName -> set of indexValues
	// Used for efficient removal when an object is updated/deleted
	reverse map[store.ObjectKey]map[string]map[string]struct{}
}

// NewIndexer creates a new Indexer.
func NewIndexer() *Indexer {
	return &Indexer{
		funcs:   make(map[schema.GroupVersionKind]map[string]IndexerFunc),
		data:    make(map[schema.GroupVersionKind]map[string]map[string]map[store.ObjectKey]struct{}),
		reverse: make(map[store.ObjectKey]map[string]map[string]struct{}),
	}
}

// AddIndex registers a new index for the given GVK and field name.
// Returns an error if the index already exists.
func (idx *Indexer) AddIndex(gvk schema.GroupVersionKind, field string, fn IndexerFunc) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if _, ok := idx.funcs[gvk]; !ok {
		idx.funcs[gvk] = make(map[string]IndexerFunc)
	}
	if _, exists := idx.funcs[gvk][field]; exists {
		return fmt.Errorf("index already exists for GVK %v field %q", gvk, field)
	}
	idx.funcs[gvk][field] = fn
	return nil
}

// Lookup returns all ObjectKeys matching the given GVK, field, and value.
func (idx *Indexer) Lookup(gvk schema.GroupVersionKind, field string, value string) []store.ObjectKey {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	gvkData, ok := idx.data[gvk]
	if !ok {
		return nil
	}
	fieldData, ok := gvkData[field]
	if !ok {
		return nil
	}
	keySet, ok := fieldData[value]
	if !ok {
		return nil
	}

	result := make([]store.ObjectKey, 0, len(keySet))
	for k := range keySet {
		result = append(result, k)
	}
	return result
}

// UpdateIndex updates all indexes for the given object.
// Removes old index entries and adds new ones.
func (idx *Indexer) UpdateIndex(key store.ObjectKey, obj client.Object) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	gvk := key.GVK

	// Remove old entries for this key using the reverse index.
	if fieldMap, ok := idx.reverse[key]; ok {
		for field, oldValues := range fieldMap {
			for oldVal := range oldValues {
				if gvkData, ok := idx.data[gvk]; ok {
					if fieldData, ok := gvkData[field]; ok {
						delete(fieldData[oldVal], key)
						if len(fieldData[oldVal]) == 0 {
							delete(fieldData, oldVal)
						}
					}
				}
			}
		}
		delete(idx.reverse, key)
	}

	// Add new entries using registered index functions.
	funcsForGVK, ok := idx.funcs[gvk]
	if !ok {
		return
	}

	for field, fn := range funcsForGVK {
		values := fn(obj)
		if len(values) == 0 {
			continue
		}

		// Ensure data maps exist.
		if _, ok := idx.data[gvk]; !ok {
			idx.data[gvk] = make(map[string]map[string]map[store.ObjectKey]struct{})
		}
		if _, ok := idx.data[gvk][field]; !ok {
			idx.data[gvk][field] = make(map[string]map[store.ObjectKey]struct{})
		}

		// Ensure reverse map exists.
		if _, ok := idx.reverse[key]; !ok {
			idx.reverse[key] = make(map[string]map[string]struct{})
		}
		if _, ok := idx.reverse[key][field]; !ok {
			idx.reverse[key][field] = make(map[string]struct{})
		}

		for _, val := range values {
			if _, ok := idx.data[gvk][field][val]; !ok {
				idx.data[gvk][field][val] = make(map[store.ObjectKey]struct{})
			}
			idx.data[gvk][field][val][key] = struct{}{}
			idx.reverse[key][field][val] = struct{}{}
		}
	}
}

// RemoveIndex removes all index entries for the given object key.
func (idx *Indexer) RemoveIndex(key store.ObjectKey) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	gvk := key.GVK

	fieldMap, ok := idx.reverse[key]
	if !ok {
		return
	}

	for field, oldValues := range fieldMap {
		for oldVal := range oldValues {
			if gvkData, ok := idx.data[gvk]; ok {
				if fieldData, ok := gvkData[field]; ok {
					delete(fieldData[oldVal], key)
					if len(fieldData[oldVal]) == 0 {
						delete(fieldData, oldVal)
					}
				}
			}
		}
	}
	delete(idx.reverse, key)
}
