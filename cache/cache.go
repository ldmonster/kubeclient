// Package cache implements the caching layer for KubeClient.
// It provides the Cache interface and its main implementation backed by
// the deduplicated store and informer machinery.
package cache

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ldmonster/kubeclient/codec"
	"github.com/ldmonster/kubeclient/store"
)

// Cache provides read access to a deduplicated Kubernetes object store.
// It manages informers for watched GVKs and provides Get/List operations.
type Cache struct {
	mu             sync.RWMutex
	store          store.Store
	codec          *codec.Codec
	informerMap    *InformerMap
	indexer        *Indexer
	scheme         *runtime.Scheme
	started        bool
	reconstructLRU *lruCache // nil if disabled
}

// NewCache creates a new Cache.
// reconstructLRUSize controls the optional LRU reconstruction cache:
//   - 0 (or negative) disables the LRU cache entirely.
//   - A positive value sets the maximum number of reconstructed objects to keep.
func NewCache(
	dynamicClient dynamic.Interface,
	scheme *runtime.Scheme,
	namespaces []string,
	reconstructLRUSize int,
) *Cache {
	s := store.NewDedupStore()
	indexer := NewIndexer()
	informerMap := NewInformerMap(dynamicClient, s, indexer, namespaces)

	var lru *lruCache
	if reconstructLRUSize > 0 {
		lru = newLRUCache(reconstructLRUSize)
	}

	return &Cache{
		store:          s,
		codec:          codec.NewCodec(scheme),
		informerMap:    informerMap,
		indexer:        indexer,
		scheme:         scheme,
		reconstructLRU: lru,
	}
}

// lruKey builds the LRU cache key for an object: "group/version/kind/namespace/name".
func lruKey(gvk schema.GroupVersionKind, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind, namespace, name)
}

// Get retrieves a single object by key from the cache.
// The GVK is determined from the `obj` type using the scheme.
// Returns an error if the object is not found or the GVK cannot be determined.
func (c *Cache) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	gvk, err := c.codec.GVKForObject(obj)
	if err != nil {
		return fmt.Errorf("cache: Get: cannot determine GVK: %w", err)
	}

	if err := c.EnsureInformer(ctx, obj); err != nil {
		return fmt.Errorf("cache: Get: ensure informer failed: %w", err)
	}

	storeKey := store.ObjectKey{
		GVK:       gvk,
		Namespace: key.Namespace,
		Name:      key.Name,
	}

	// Check the LRU reconstruction cache first.
	if c.reconstructLRU != nil {
		cacheKey := lruKey(gvk, key.Namespace, key.Name)
		if cached, ok := c.reconstructLRU.get(cacheKey); ok {
			if u, ok2 := cached.(*unstructured.Unstructured); ok2 {
				return c.codec.FromUnstructured(u, obj)
			}
		}

		// Cache miss — reconstruct from the dedup store.
		u, found := c.store.Get(storeKey)
		if !found {
			return fmt.Errorf("cache: Get: object %s/%s not found", key.Namespace, key.Name)
		}
		c.reconstructLRU.put(cacheKey, u)
		return c.codec.FromUnstructured(u, obj)
	}

	// LRU disabled — reconstruct directly.
	u, found := c.store.Get(storeKey)
	if !found {
		return fmt.Errorf("cache: Get: object %s/%s not found", key.Namespace, key.Name)
	}

	return c.codec.FromUnstructured(u, obj)
}

// invalidateLRU removes the LRU entry for the given object key (if LRU is enabled).
func (c *Cache) invalidateLRU(gvk schema.GroupVersionKind, namespace, name string) {
	if c.reconstructLRU != nil {
		c.reconstructLRU.delete(lruKey(gvk, namespace, name))
	}
}

// List retrieves all objects of the given type from the cache.
// Supports label selector filtering via client.ListOptions.
func (c *Cache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	gvk, err := c.gvkForList(list)
	if err != nil {
		return fmt.Errorf("cache: List: cannot determine GVK: %w", err)
	}

	// Ensure informer exists for the item GVK.
	// Create a dummy unstructured to pass to EnsureInformer.
	dummy := &unstructured.Unstructured{}
	dummy.SetGroupVersionKind(gvk)
	if err := c.EnsureInformer(ctx, dummy); err != nil {
		return fmt.Errorf("cache: List: ensure informer failed: %w", err)
	}

	// Parse client.ListOptions.
	listOpts := &client.ListOptions{}
	for _, opt := range opts {
		opt.ApplyToList(listOpts)
	}

	// Build store.ListOption slice.
	var storeOpts []store.ListOption
	if listOpts.Namespace != "" {
		storeOpts = append(storeOpts, store.WithNamespace(listOpts.Namespace))
	}
	if listOpts.LabelSelector != nil {
		reqs, selectable := listOpts.LabelSelector.Requirements()
		if selectable {
			labelMap := make(map[string]string)
			for _, req := range reqs {
				op := req.Operator()
				if op == selection.Equals || op == selection.DoubleEquals || op == selection.In {
					vals := req.Values()
					if vals.Len() == 1 {
						labelMap[req.Key()] = vals.List()[0]
					}
				}
			}
			if len(labelMap) > 0 {
				storeOpts = append(storeOpts, store.WithLabelSelector(labelMap))
			}
		}
	}
	if listOpts.FieldSelector != nil {
		reqs := listOpts.FieldSelector.Requirements()
		for _, req := range reqs {
			if req.Operator == "=" || req.Operator == "==" {
				storeOpts = append(storeOpts, store.WithFieldSelector(req.Field, req.Value))
			}
		}
	}

	items := c.store.List(gvk, storeOpts...)

	// If the list is an UnstructuredList, populate directly.
	if ul, ok := list.(*unstructured.UnstructuredList); ok {
		ul.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   gvk.Group,
			Version: gvk.Version,
			Kind:    gvk.Kind + "List",
		})
		ul.Items = make([]unstructured.Unstructured, 0, len(items))
		for _, u := range items {
			ul.Items = append(ul.Items, *u)
		}
		return nil
	}

	// For typed lists, try to create item instances via the scheme.
	robj, err := c.scheme.New(gvk)
	if err != nil {
		// Fall back: populate as unstructured items in the list.
		return c.populateListFromUnstructured(list, items, gvk)
	}

	itemObj, ok := robj.(client.Object)
	if !ok {
		return c.populateListFromUnstructured(list, items, gvk)
	}

	// Convert each unstructured item to the typed object.
	extractedItems := make([]runtime.Object, 0, len(items))
	for _, u := range items {
		newItem := itemObj.DeepCopyObject().(client.Object)
		if err := c.codec.FromUnstructured(u, newItem); err != nil {
			return fmt.Errorf("cache: List: FromUnstructured failed: %w", err)
		}
		extractedItems = append(extractedItems, newItem)
	}

	return meta.SetList(list, extractedItems)
}

// populateListFromUnstructured populates a list using unstructured items.
// Used as a fallback when the scheme doesn't know the item type.
func (c *Cache) populateListFromUnstructured(list client.ObjectList, items []*unstructured.Unstructured, gvk schema.GroupVersionKind) error {
	ul := &unstructured.UnstructuredList{}
	ul.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind + "List",
	})
	for _, u := range items {
		ul.Items = append(ul.Items, *u)
	}
	// Try to convert the UnstructuredList to the target list type.
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(ul.Object, list); err != nil {
		return fmt.Errorf("cache: List: cannot populate list: %w", err)
	}
	return nil
}

// gvkForList determines the item GVK from a list object.
// It strips the "List" suffix from the Kind and looks up the GVK in the scheme.
func (c *Cache) gvkForList(list client.ObjectList) (schema.GroupVersionKind, error) {
	// If it's an UnstructuredList, read GVK directly.
	if ul, ok := list.(*unstructured.UnstructuredList); ok {
		gvk := ul.GroupVersionKind()
		// Strip "List" suffix.
		gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")
		if gvk.Kind != "" {
			return gvk, nil
		}
		return schema.GroupVersionKind{}, fmt.Errorf("cache: gvkForList: UnstructuredList has no GVK set")
	}

	// For typed lists, look up in scheme.
	gvks, _, err := c.scheme.ObjectKinds(list)
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("cache: gvkForList: scheme lookup failed: %w", err)
	}
	for _, gvk := range gvks {
		if gvk.Kind != "" {
			// Strip "List" suffix to get item GVK.
			itemKind := strings.TrimSuffix(gvk.Kind, "List")
			return schema.GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    itemKind,
			}, nil
		}
	}
	return schema.GroupVersionKind{}, fmt.Errorf("cache: gvkForList: no GVK found for list type")
}

// IndexField adds an index for the given field on the given object type.
// Must be called before Start().
func (c *Cache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	gvk, err := c.codec.GVKForObject(obj)
	if err != nil {
		return fmt.Errorf("cache: IndexField: cannot determine GVK: %w", err)
	}

	wrappedFn := IndexerFunc(func(o client.Object) []string {
		return extractValue(o)
	})

	return c.indexer.AddIndex(gvk, field, wrappedFn)
}

// EnsureInformer ensures an informer exists for the given object type.
// Creates one if it doesn't exist yet.
func (c *Cache) EnsureInformer(ctx context.Context, obj client.Object) error {
	gvk, err := c.codec.GVKForObject(obj)
	if err != nil {
		return fmt.Errorf("cache: EnsureInformer: cannot determine GVK: %w", err)
	}

	inf := c.informerMap.GetOrCreate(gvk)

	// If the cache is already started, start the new informer immediately.
	c.mu.RLock()
	started := c.started
	c.mu.RUnlock()

	if started && !inf.HasSynced() {
		go func() {
			_ = inf.Run(ctx)
		}()
	}

	return nil
}

// Start starts all registered informers. Blocks until ctx is cancelled.
func (c *Cache) Start(ctx context.Context) error {
	c.mu.Lock()
	c.started = true
	c.mu.Unlock()

	c.informerMap.Start(ctx)

	<-ctx.Done()
	return ctx.Err()
}

// WaitForCacheSync waits for all informers to sync.
func (c *Cache) WaitForCacheSync(ctx context.Context) bool {
	return c.informerMap.WaitForCacheSync(ctx)
}
