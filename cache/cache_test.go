package cache

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ldmonster/kubeclient/store"
)

// newTestCache creates a Cache backed by a fake dynamic client and an empty scheme.
func newTestCache() *Cache {
	scheme := runtime.NewScheme()
	dynClient := fake.NewSimpleDynamicClient(scheme)
	return NewCache(dynClient, scheme, nil, 0)
}

// makeTestUnstructured creates an Unstructured object with the given GVK, name, and namespace.
func makeTestUnstructured(gvk schema.GroupVersionKind, name, namespace string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetName(name)
	u.SetNamespace(namespace)
	return u
}

// makeStoreKey builds a store.ObjectKey for use in tests.
func makeStoreKey(gvk schema.GroupVersionKind, name, namespace string) store.ObjectKey {
	return store.ObjectKey{GVK: gvk, Name: name, Namespace: namespace}
}

func TestNewCache(t *testing.T) {
	c := newTestCache()
	if c == nil {
		t.Fatal("NewCache returned nil")
	}
	if c.store == nil {
		t.Error("Cache.store is nil")
	}
	if c.codec == nil {
		t.Error("Cache.codec is nil")
	}
	if c.informerMap == nil {
		t.Error("Cache.informerMap is nil")
	}
	if c.indexer == nil {
		t.Error("Cache.indexer is nil")
	}
}

func TestCache_IndexField_RegistersWithoutError(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	// Use an Unstructured object so GVK is read from TypeMeta.
	obj := makeTestUnstructured(
		schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		"", "",
	)

	err := c.IndexField(ctx, obj, "metadata.namespace", func(o client.Object) []string {
		return []string{o.GetNamespace()}
	})
	if err != nil {
		t.Fatalf("IndexField returned unexpected error: %v", err)
	}
}

func TestCache_IndexField_DuplicateReturnsError(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	obj := makeTestUnstructured(
		schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		"", "",
	)
	fn := func(o client.Object) []string { return []string{o.GetNamespace()} }

	if err := c.IndexField(ctx, obj, "namespace", fn); err != nil {
		t.Fatalf("first IndexField failed: %v", err)
	}
	if err := c.IndexField(ctx, obj, "namespace", fn); err == nil {
		t.Error("second IndexField for same GVK+field should return error, got nil")
	}
}

func TestCache_EnsureInformer_CreatesInformerEntry(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	obj := makeTestUnstructured(
		schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		"deploy-1", "default",
	)

	err := c.EnsureInformer(ctx, obj)
	if err != nil {
		t.Fatalf("EnsureInformer returned unexpected error: %v", err)
	}

	// Calling again for the same GVK should be idempotent.
	err = c.EnsureInformer(ctx, obj)
	if err != nil {
		t.Fatalf("second EnsureInformer returned unexpected error: %v", err)
	}
}

func TestCache_EnsureInformer_DifferentGVKs(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	deploy := makeTestUnstructured(
		schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		"", "",
	)
	pod := makeTestUnstructured(
		schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"},
		"", "",
	)

	if err := c.EnsureInformer(ctx, deploy); err != nil {
		t.Fatalf("EnsureInformer(Deployment) failed: %v", err)
	}
	if err := c.EnsureInformer(ctx, pod); err != nil {
		t.Fatalf("EnsureInformer(Pod) failed: %v", err)
	}
}

func TestCache_Get_ReturnsErrorWhenNotFound(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	into := makeTestUnstructured(
		schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		"", "",
	)

	err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "nonexistent"}, into)
	if err == nil {
		t.Fatal("Get should return an error for a non-existent object, got nil")
	}
}

func TestCache_Get_ReturnsObjectAfterUpsert(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	obj := makeTestUnstructured(gvk, "deploy-1", "default")

	// Directly upsert into the underlying store to bypass the informer.
	key := makeStoreKey(gvk, "deploy-1", "default")
	if err := c.store.Upsert(key, obj); err != nil {
		t.Fatalf("store.Upsert failed: %v", err)
	}

	into := &unstructured.Unstructured{}
	into.SetGroupVersionKind(gvk)

	err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "deploy-1"}, into)
	if err != nil {
		t.Fatalf("Get returned unexpected error: %v", err)
	}
	if into.GetName() != "deploy-1" {
		t.Errorf("Get returned object with name %q, want %q", into.GetName(), "deploy-1")
	}
	if into.GetNamespace() != "default" {
		t.Errorf("Get returned object with namespace %q, want %q", into.GetNamespace(), "default")
	}
}

func TestCache_List_ReturnsEmptyWhenStoreEmpty(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind + "List",
	})

	err := c.List(ctx, list)
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}
	if len(list.Items) != 0 {
		t.Errorf("List returned %d items, want 0", len(list.Items))
	}
}

func TestCache_List_ReturnsItemsAfterUpsert(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

	// Upsert two objects directly into the store.
	for _, name := range []string{"deploy-1", "deploy-2"} {
		obj := makeTestUnstructured(gvk, name, "default")
		key := makeStoreKey(gvk, name, "default")
		if err := c.store.Upsert(key, obj); err != nil {
			t.Fatalf("store.Upsert(%s) failed: %v", name, err)
		}
	}

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind + "List",
	})

	err := c.List(ctx, list)
	if err != nil {
		t.Fatalf("List returned unexpected error: %v", err)
	}
	if len(list.Items) != 2 {
		t.Errorf("List returned %d items, want 2", len(list.Items))
	}
}

func TestCache_WaitForCacheSync_ReturnsFalseWhenNotStarted(t *testing.T) {
	c := newTestCache()

	// Register an informer but do not start the cache.
	ctx := context.Background()
	obj := makeTestUnstructured(
		schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		"", "",
	)
	_ = c.EnsureInformer(ctx, obj)

	// Use a short timeout — informer is not running so it won't sync.
	syncCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	synced := c.WaitForCacheSync(syncCtx)
	if synced {
		t.Error("WaitForCacheSync should return false when cache has not been started")
	}
}

func TestCache_WaitForCacheSync_ReturnsTrueWhenNoInformers(t *testing.T) {
	c := newTestCache()

	// No informers registered — vacuously all synced.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	synced := c.WaitForCacheSync(ctx)
	if !synced {
		t.Error("WaitForCacheSync should return true when there are no informers")
	}
}

func TestCache_IndexField_NoGVKReturnsError(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	// An Unstructured with no GVK set — codec.GVKForObject should fail.
	obj := &unstructured.Unstructured{}

	err := c.IndexField(ctx, obj, "namespace", func(o client.Object) []string {
		return []string{o.GetNamespace()}
	})
	if err == nil {
		t.Error("IndexField with no GVK set should return an error, got nil")
	}
}

func TestCache_List_UnstructuredListWithNoGVKReturnsError(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	// An UnstructuredList with no GVK set — gvkForList should fail.
	list := &unstructured.UnstructuredList{}

	err := c.List(ctx, list)
	if err == nil {
		t.Error("List with UnstructuredList having no GVK should return an error, got nil")
	}
}
