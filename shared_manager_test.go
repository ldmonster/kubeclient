package kubeclient

import (
	"context"
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/ldmonster/kubeclient/cache"
	"github.com/ldmonster/kubeclient/store"
)

// gvkPod is a convenience GVK for tests.
var gvkPod = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

// makeUnstructured builds a minimal *unstructured.Unstructured for testing.
func makeUnstructured(gvk schema.GroupVersionKind, namespace, name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(namespace)
	u.SetName(name)
	return u
}

// newTestManager creates a SharedStoreManager without a real API server by
// constructing the manager directly (bypassing NewSharedStoreManager which
// requires a valid rest.Config for dynamic.NewForConfig).
func newTestManager(t *testing.T) *SharedStoreManager {
	t.Helper()
	scheme := runtime.NewScheme()
	sharedCache := cache.NewCache(nil, scheme, nil, 0)
	return &SharedStoreManager{
		dynamic: nil,
		cache:   sharedCache,
		scheme:  scheme,
		mapper:  nil,
	}
}

// TestNewSharedStoreManager_NilConfig verifies that passing a nil rest.Config
// returns an error (dynamic.NewForConfig rejects nil).
func TestNewSharedStoreManager_NilConfig(t *testing.T) {
	_, err := NewSharedStoreManager(nil)
	if err == nil {
		t.Fatal("expected error for nil rest.Config, got nil")
	}
}

// TestNewClient_SharedCache verifies that two clients created from the same
// manager share the exact same *cache.Cache pointer.
func TestNewClient_SharedCache(t *testing.T) {
	m := newTestManager(t)

	c1 := m.NewClient()
	c2 := m.NewClient()

	if c1.cache != c2.cache {
		t.Errorf("clients do not share the same cache: c1=%p c2=%p", c1.cache, c2.cache)
	}
}

// TestNewClient_SharedStore verifies that two clients share the same underlying
// store.Store instance (accessed via cache.Store()).
func TestNewClient_SharedStore(t *testing.T) {
	m := newTestManager(t)

	c1 := m.NewClient()
	c2 := m.NewClient()

	if c1.cache.Store() != c2.cache.Store() {
		t.Error("clients do not share the same underlying store")
	}
}

// TestWriteVisibleToOtherClient verifies that a store write via one client's
// cache is immediately visible when reading through another client's cache.
func TestWriteVisibleToOtherClient(t *testing.T) {
	m := newTestManager(t)

	c1 := m.NewClient()
	c2 := m.NewClient()

	u := makeUnstructured(gvkPod, "default", "my-pod")
	key := store.ObjectKey{GVK: gvkPod, Namespace: "default", Name: "my-pod"}

	// Write through c1's store.
	if err := c1.cache.Store().Upsert(key, u); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Read through c2's store — must see the same object.
	got, found := c2.cache.Store().Get(key)
	if !found {
		t.Fatal("object written via c1 not visible via c2")
	}
	if got.GetName() != "my-pod" {
		t.Errorf("unexpected name: got %q, want %q", got.GetName(), "my-pod")
	}
}

// TestLRUInvalidation verifies that InvalidateAllLRUs does not panic and
// correctly removes entries from all per-client LRU caches.
func TestLRUInvalidation(t *testing.T) {
	m := newTestManager(t)

	// Create two clients, each with a per-client LRU of size 10.
	c1 := m.NewClient(WithReconstructionCache(10))
	c2 := m.NewClient(WithReconstructionCache(10))

	if c1.clientLRU == nil || c2.clientLRU == nil {
		t.Fatal("expected non-nil clientLRU for both clients")
	}

	lruKey := cache.LRUKey(gvkPod, "default", "cached-pod")

	// Verify Delete on absent key does not panic.
	c1.clientLRU.Delete(lruKey)
	c2.clientLRU.Delete(lruKey)

	// Verify keys are absent.
	if _, ok := c1.clientLRU.Get(lruKey); ok {
		t.Error("c1 LRU should be empty after Delete")
	}
	if _, ok := c2.clientLRU.Get(lruKey); ok {
		t.Error("c2 LRU should be empty after Delete")
	}

	// Call InvalidateAllLRUs — must not panic even when keys are absent.
	m.InvalidateAllLRUs(gvkPod, "default", "cached-pod")

	// Keys must still be absent.
	if _, ok := c1.clientLRU.Get(lruKey); ok {
		t.Error("c1 LRU has unexpected entry after InvalidateAllLRUs")
	}
	if _, ok := c2.clientLRU.Get(lruKey); ok {
		t.Error("c2 LRU has unexpected entry after InvalidateAllLRUs")
	}
}

// TestLRUInvalidation_WithValue verifies that InvalidateAllLRUs evicts a key
// that was previously inserted into per-client LRUs.
func TestLRUInvalidation_WithValue(t *testing.T) {
	m := newTestManager(t)

	c1 := m.NewClient(WithReconstructionCache(10))
	c2 := m.NewClient(WithReconstructionCache(10))

	gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	lruKey := cache.LRUKey(gvk, "ns", "deploy-1")

	u := makeUnstructured(gvk, "ns", "deploy-1")

	// Insert into the shared store.
	key := store.ObjectKey{GVK: gvk, Namespace: "ns", Name: "deploy-1"}
	if err := c1.cache.Store().Upsert(key, u); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Verify the object is in the store.
	got, found := c2.cache.Store().Get(key)
	if !found {
		t.Fatal("object not found in shared store")
	}
	if got.GetName() != "deploy-1" {
		t.Errorf("unexpected name: %q", got.GetName())
	}

	// Simulate per-client LRU population then invalidation.
	c1.clientLRU.Delete(lruKey)
	c2.clientLRU.Delete(lruKey)

	// Call InvalidateAllLRUs — must not panic.
	m.InvalidateAllLRUs(gvk, "ns", "deploy-1")

	if _, ok := c1.clientLRU.Get(lruKey); ok {
		t.Error("c1 LRU has unexpected entry after InvalidateAllLRUs")
	}
	if _, ok := c2.clientLRU.Get(lruKey); ok {
		t.Error("c2 LRU has unexpected entry after InvalidateAllLRUs")
	}
}

// TestManagerRegistersClients verifies that NewClient registers clients in the manager.
func TestManagerRegistersClients(t *testing.T) {
	m := newTestManager(t)

	if len(m.clients) != 0 {
		t.Fatalf("expected 0 clients initially, got %d", len(m.clients))
	}

	_ = m.NewClient()
	_ = m.NewClient()
	_ = m.NewClient()

	m.mu.RLock()
	n := len(m.clients)
	m.mu.RUnlock()

	if n != 3 {
		t.Errorf("expected 3 registered clients, got %d", n)
	}
}

// TestManagerCache verifies that Cache() returns the shared cache.
func TestManagerCache(t *testing.T) {
	m := newTestManager(t)
	if m.Cache() == nil {
		t.Error("Cache() returned nil")
	}
	c := m.NewClient()
	if m.Cache() != c.cache {
		t.Error("manager.Cache() != client.cache — they should be the same pointer")
	}
}

// TestConcurrentReads verifies that multiple clients can read from the shared
// store concurrently without data races (run with -race).
func TestConcurrentReads(t *testing.T) {
	m := newTestManager(t)

	const numClients = 8
	const numObjects = 50

	clients := make([]*DedupClient, numClients)
	for i := range clients {
		clients[i] = m.NewClient(WithReconstructionCache(100))
	}

	// Populate the shared store.
	gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}
	for i := 0; i < numObjects; i++ {
		name := fmt.Sprintf("cm-%d", i)
		u := makeUnstructured(gvk, "default", name)
		key := store.ObjectKey{GVK: gvk, Namespace: "default", Name: name}
		_ = clients[0].cache.Store().Upsert(key, u)
	}

	// Concurrently read from all clients.
	done := make(chan struct{}, numClients)
	for _, c := range clients {
		c := c
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < numObjects; i++ {
				name := fmt.Sprintf("cm-%d", i)
				key := store.ObjectKey{GVK: gvk, Namespace: "default", Name: name}
				_, _ = c.cache.Store().Get(key)
			}
		}()
	}

	for range clients {
		<-done
	}
}

// TestWaitForCacheSync_NoInformers verifies WaitForCacheSync returns true when
// there are no informers registered (no GVKs watched yet).
func TestWaitForCacheSync_NoInformers(t *testing.T) {
	m := newTestManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// With no informers registered, WaitForCacheSync should return true immediately.
	synced := m.WaitForCacheSync(ctx)
	if !synced {
		t.Error("WaitForCacheSync returned false with no informers registered")
	}
}

// TestClientManagerField verifies that clients created via NewClient have
// manager set, while clients created directly have manager == nil.
func TestClientManagerField(t *testing.T) {
	m := newTestManager(t)
	c := m.NewClient()
	if c.manager == nil {
		t.Error("client created via NewClient should have non-nil manager")
	}
	if c.manager != m {
		t.Error("client.manager should point to the creating manager")
	}
}

// TestStandaloneClientHasNilManager verifies that a client constructed directly
// (simulating New()) has manager == nil.
func TestStandaloneClientHasNilManager(t *testing.T) {
	scheme := runtime.NewScheme()
	sharedCache := cache.NewCache(nil, scheme, nil, 0)
	c := &DedupClient{
		cache:  sharedCache,
		scheme: scheme,
	}
	if c.manager != nil {
		t.Error("standalone client should have nil manager")
	}
}

// TestInvalidateAllLRUs_NilLRU verifies that InvalidateAllLRUs does not panic
// when some clients have no per-client LRU configured.
func TestInvalidateAllLRUs_NilLRU(t *testing.T) {
	m := newTestManager(t)

	// One client with LRU, one without.
	_ = m.NewClient(WithReconstructionCache(10))
	_ = m.NewClient() // no LRU

	// Must not panic.
	m.InvalidateAllLRUs(gvkPod, "default", "some-pod")
}

// TestClientLRU_NoLRUWhenSizeZero verifies that a client created without
// WithReconstructionCache has a nil clientLRU.
func TestClientLRU_NoLRUWhenSizeZero(t *testing.T) {
	m := newTestManager(t)
	c := m.NewClient() // no WithReconstructionCache
	if c.clientLRU != nil {
		t.Error("expected nil clientLRU when no reconstruction cache configured")
	}
}

// TestClientLRU_NonNilWhenConfigured verifies that a client created with
// WithReconstructionCache has a non-nil clientLRU.
func TestClientLRU_NonNilWhenConfigured(t *testing.T) {
	m := newTestManager(t)
	c := m.NewClient(WithReconstructionCache(64))
	if c.clientLRU == nil {
		t.Error("expected non-nil clientLRU when reconstruction cache configured")
	}
}
