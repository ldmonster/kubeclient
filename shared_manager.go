// Package kubeclient provides a Kubernetes client with a deduplicated cache.
// This file implements SharedStoreManager, which allows multiple DedupClient
// instances to share a single DedupStore, InformerMap, and Indexer.
package kubeclient

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/ldmonster/kubeclient/cache"
	"github.com/ldmonster/kubeclient/codec"
)

// SharedStoreManager manages a single DedupStore shared across multiple DedupClients.
// All clients registered with the manager share the same store, informer map, and indexer,
// reducing memory usage and API server load proportionally to the number of clients.
//
// Each client retains its own LRU reconstruction cache for per-client hot-path optimization.
// When a write-through operation updates the shared store, all per-client LRU caches are
// invalidated for the affected object key.
type SharedStoreManager struct {
	mu      sync.RWMutex
	dynamic dynamic.Interface
	cache   *cache.Cache
	scheme  *runtime.Scheme
	mapper  meta.RESTMapper
	clients []*DedupClient
}

// NewSharedStoreManager creates a SharedStoreManager from a rest.Config.
// The manager owns a single DedupStore, InformerMap, and Indexer shared by all clients.
// Per-client LRU caches are configured individually via NewClient options.
func NewSharedStoreManager(cfg *rest.Config, optFns ...Option) (*SharedStoreManager, error) {
	if cfg == nil {
		return nil, fmt.Errorf("kubeclient: SharedStoreManager: rest.Config must not be nil")
	}

	opts := defaultOptions()
	for _, fn := range optFns {
		fn(opts)
	}
	if opts.scheme == nil {
		opts.scheme = runtime.NewScheme()
	}

	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubeclient: SharedStoreManager: failed to create dynamic client: %w", err)
	}

	var mapper meta.RESTMapper
	if opts.mapper != nil {
		mapper = opts.mapper
	} else {
		mapper = meta.NewDefaultRESTMapper(nil)
	}

	// Single shared cache (store + informer map + indexer).
	// LRU is disabled on the shared cache itself — per-client LRUs handle hot-path caching.
	sharedCache := cache.NewCache(dynClient, opts.scheme, opts.namespaces, 0)

	return &SharedStoreManager{
		dynamic: dynClient,
		cache:   sharedCache,
		scheme:  opts.scheme,
		mapper:  mapper,
	}, nil
}

// NewClient creates a new DedupClient backed by the shared store.
// Each client gets its own LRU cache (controlled by WithReconstructionCache option)
// but shares the underlying DedupStore, InformerMap, and Indexer.
func (m *SharedStoreManager) NewClient(optFns ...Option) *DedupClient {
	opts := defaultOptions()
	for _, fn := range optFns {
		fn(opts)
	}

	// Per-client LRU cache — wraps the shared cache's store for hot-path reads.
	var lru *cache.LRUCache
	if opts.reconstructLRUSize > 0 {
		lru = cache.NewLRUCache(opts.reconstructLRUSize)
	}

	c := &DedupClient{
		dynamic:   m.dynamic,
		cache:     m.cache,
		clientLRU: lru,
		codec:     codec.NewCodec(m.scheme),
		scheme:    m.scheme,
		mapper:    m.mapper,
		opts:      opts,
		manager:   m,
	}

	m.mu.Lock()
	m.clients = append(m.clients, c)
	m.mu.Unlock()

	return c
}

// InvalidateAllLRUs invalidates the LRU entry for the given key across all registered clients.
// Called after every write-through store operation so stale per-client LRU entries are evicted.
func (m *SharedStoreManager) InvalidateAllLRUs(gvk schema.GroupVersionKind, namespace, name string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := cache.LRUKey(gvk, namespace, name)
	for _, c := range m.clients {
		if c.clientLRU != nil {
			c.clientLRU.Delete(key)
		}
	}
}

// Start starts all informers. Blocks until ctx is cancelled.
// Call this once for the entire manager — all clients share the same informers.
func (m *SharedStoreManager) Start(ctx context.Context) error {
	return m.cache.Start(ctx)
}

// WaitForCacheSync waits for all informers to sync.
func (m *SharedStoreManager) WaitForCacheSync(ctx context.Context) bool {
	return m.cache.WaitForCacheSync(ctx)
}

// Cache returns the shared cache for direct access.
func (m *SharedStoreManager) Cache() *cache.Cache {
	return m.cache
}
