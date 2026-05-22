package cache

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"

	"github.com/ldmonster/kubeclient/store"
)

// newFakeDynamicClient creates a fake dynamic client with an empty scheme.
func newFakeDynamicClient() *fake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	return fake.NewSimpleDynamicClient(scheme)
}

func TestNewInformerMap(t *testing.T) {
	dynClient := newFakeDynamicClient()
	s := store.NewDedupStore()
	indexer := NewIndexer()

	m := NewInformerMap(dynClient, s, indexer, nil)
	if m == nil {
		t.Fatal("NewInformerMap returned nil")
	}
}

func TestInformerMap_GetOrCreate_ReturnsSameInformerForSameGVK(t *testing.T) {
	dynClient := newFakeDynamicClient()
	s := store.NewDedupStore()
	indexer := NewIndexer()
	m := NewInformerMap(dynClient, s, indexer, nil)

	gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

	inf1 := m.GetOrCreate(gvk)
	inf2 := m.GetOrCreate(gvk)

	if inf1 == nil {
		t.Fatal("GetOrCreate returned nil informer")
	}
	if inf1 != inf2 {
		t.Error("GetOrCreate returned different informers for the same GVK (not idempotent)")
	}
}

func TestInformerMap_GetOrCreate_ReturnsDifferentInformersForDifferentGVKs(t *testing.T) {
	dynClient := newFakeDynamicClient()
	s := store.NewDedupStore()
	indexer := NewIndexer()
	m := NewInformerMap(dynClient, s, indexer, nil)

	gvk1 := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	gvk2 := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

	inf1 := m.GetOrCreate(gvk1)
	inf2 := m.GetOrCreate(gvk2)

	if inf1 == nil || inf2 == nil {
		t.Fatal("GetOrCreate returned nil informer")
	}
	if inf1 == inf2 {
		t.Error("GetOrCreate returned the same informer for different GVKs")
	}
}

func TestInformerMap_GetOrCreate_MultipleGVKs(t *testing.T) {
	dynClient := newFakeDynamicClient()
	s := store.NewDedupStore()
	indexer := NewIndexer()
	m := NewInformerMap(dynClient, s, indexer, nil)

	gvks := []schema.GroupVersionKind{
		{Group: "apps", Version: "v1", Kind: "Deployment"},
		{Group: "", Version: "v1", Kind: "Pod"},
		{Group: "", Version: "v1", Kind: "Service"},
		{Group: "batch", Version: "v1", Kind: "Job"},
	}

	informers := make([]*GVKInformer, len(gvks))
	for i, gvk := range gvks {
		informers[i] = m.GetOrCreate(gvk)
		if informers[i] == nil {
			t.Fatalf("GetOrCreate(%v) returned nil", gvk)
		}
	}

	// Verify idempotency: calling again returns the same pointer.
	for i, gvk := range gvks {
		again := m.GetOrCreate(gvk)
		if again != informers[i] {
			t.Errorf("GetOrCreate(%v) second call returned different pointer", gvk)
		}
	}

	// Verify all informers are distinct.
	for i := 0; i < len(informers); i++ {
		for j := i + 1; j < len(informers); j++ {
			if informers[i] == informers[j] {
				t.Errorf("informers[%d] and informers[%d] are the same pointer (GVKs: %v, %v)",
					i, j, gvks[i], gvks[j])
			}
		}
	}
}

func TestInformerMap_WaitForCacheSync_ReturnsFalseWhenContextCancelled(t *testing.T) {
	dynClient := newFakeDynamicClient()
	s := store.NewDedupStore()
	indexer := NewIndexer()
	m := NewInformerMap(dynClient, s, indexer, nil)

	// Create an informer but do NOT run it — it will never sync.
	gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	m.GetOrCreate(gvk)

	// Cancel the context immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	synced := m.WaitForCacheSync(ctx)
	if synced {
		t.Error("WaitForCacheSync should return false when context is already cancelled")
	}
}

func TestInformerMap_WaitForCacheSync_ReturnsTrueWhenNoInformers(t *testing.T) {
	dynClient := newFakeDynamicClient()
	s := store.NewDedupStore()
	indexer := NewIndexer()
	m := NewInformerMap(dynClient, s, indexer, nil)

	// No informers registered — all (zero) are synced, so should return true quickly.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	synced := m.WaitForCacheSync(ctx)
	if !synced {
		t.Error("WaitForCacheSync should return true when there are no informers")
	}
}

func TestInformerMap_WaitForCacheSync_ReturnsFalseWithTimeout(t *testing.T) {
	dynClient := newFakeDynamicClient()
	s := store.NewDedupStore()
	indexer := NewIndexer()
	m := NewInformerMap(dynClient, s, indexer, nil)

	// Register an informer that will never sync (not started).
	gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}
	m.GetOrCreate(gvk)

	// Use a very short timeout — informer is not running so it won't sync.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	synced := m.WaitForCacheSync(ctx)
	if synced {
		t.Error("WaitForCacheSync should return false when informer has not synced and context times out")
	}
}

func TestInformerMap_GetOrCreate_WithNamespace(t *testing.T) {
	dynClient := newFakeDynamicClient()
	s := store.NewDedupStore()
	indexer := NewIndexer()
	// Single namespace — informer should be namespace-scoped.
	m := NewInformerMap(dynClient, s, indexer, []string{"default"})

	gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}
	inf := m.GetOrCreate(gvk)
	if inf == nil {
		t.Fatal("GetOrCreate returned nil for namespace-scoped informer map")
	}

	// Calling again should return the same informer.
	inf2 := m.GetOrCreate(gvk)
	if inf != inf2 {
		t.Error("GetOrCreate not idempotent for namespace-scoped informer map")
	}
}

func TestInformerMap_GetOrCreate_HasSyncedFalseBeforeRun(t *testing.T) {
	dynClient := newFakeDynamicClient()
	s := store.NewDedupStore()
	indexer := NewIndexer()
	m := NewInformerMap(dynClient, s, indexer, nil)

	gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	inf := m.GetOrCreate(gvk)

	if inf.HasSynced() {
		t.Error("HasSynced should be false before the informer is run")
	}
}
