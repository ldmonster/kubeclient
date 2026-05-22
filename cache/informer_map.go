// Package cache implements the caching layer for KubeClient.
// This file maintains the mapping from GroupVersionKind to its running Informer.
package cache

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/ldmonster/kubeclient/internal"
	"github.com/ldmonster/kubeclient/store"
)

// InformerMap manages GVKInformers for multiple GVKs.
// Thread-safe for concurrent use.
type InformerMap struct {
	mu         sync.RWMutex
	dynamic    dynamic.Interface
	store      store.Store
	indexer    *Indexer
	informers  map[schema.GroupVersionKind]*GVKInformer
	namespaces []string // empty = all namespaces
}

// NewInformerMap creates a new InformerMap.
func NewInformerMap(dynamicClient dynamic.Interface, s store.Store, indexer *Indexer, namespaces []string) *InformerMap {
	return &InformerMap{
		dynamic:    dynamicClient,
		store:      s,
		indexer:    indexer,
		informers:  make(map[schema.GroupVersionKind]*GVKInformer),
		namespaces: namespaces,
	}
}

// GetOrCreate returns the existing informer for the GVK, or creates a new one.
// The GVR is derived from the GVK using a simple pluralization rule.
func (m *InformerMap) GetOrCreate(gvk schema.GroupVersionKind) *GVKInformer {
	// Fast path: check with read lock.
	m.mu.RLock()
	inf, ok := m.informers[gvk]
	m.mu.RUnlock()
	if ok {
		return inf
	}

	// Slow path: create with write lock.
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock.
	if inf, ok = m.informers[gvk]; ok {
		return inf
	}

	gvr := internal.GVRFromGVK(gvk)

	// Determine namespace: use first namespace if specified, empty means all.
	namespace := ""
	if len(m.namespaces) == 1 {
		namespace = m.namespaces[0]
	}

	inf = NewGVKInformer(gvk, gvr, m.dynamic, m.store, m.indexer, namespace)
	m.informers[gvk] = inf
	return inf
}

// Start starts all registered informers. Non-blocking — each informer runs in its own goroutine.
func (m *InformerMap) Start(ctx context.Context) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, inf := range m.informers {
		go func(i *GVKInformer) {
			_ = i.Run(ctx)
		}(inf)
	}
}

// WaitForCacheSync waits until all informers have synced (hasSynced == true).
// Returns false if ctx is cancelled before sync completes.
func (m *InformerMap) WaitForCacheSync(ctx context.Context) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		m.mu.RLock()
		allSynced := true
		for _, inf := range m.informers {
			if !inf.HasSynced() {
				allSynced = false
				break
			}
		}
		m.mu.RUnlock()

		if allSynced {
			return true
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(10 * time.Millisecond):
		}
	}
}
