// Package kubeclient provides a Kubernetes client with a deduplicated cache.
// It implements the client.Client interface from sigs.k8s.io/controller-runtime,
// making it a drop-in replacement for existing controllers and operators.
package kubeclient

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ldmonster/kubeclient/cache"
	"github.com/ldmonster/kubeclient/codec"
	"github.com/ldmonster/kubeclient/internal"
	"github.com/ldmonster/kubeclient/store"
)

// DedupClient is a Kubernetes client with a deduplicated cache.
// It implements sigs.k8s.io/controller-runtime/pkg/client.Client.
type DedupClient struct {
	dynamic dynamic.Interface
	cache   *cache.Cache
	codec   *codec.Codec
	store   store.Store
	scheme  *runtime.Scheme
	mapper  meta.RESTMapper
	opts    *options
}

// Compile-time interface checks.
var _ client.Client = &DedupClient{}
var _ client.Reader = &DedupClient{}
var _ client.Writer = &DedupClient{}

// New creates a new DedupClient from a rest.Config.
func New(cfg *rest.Config, optFns ...Option) (*DedupClient, error) {
	opts := defaultOptions()
	for _, fn := range optFns {
		fn(opts)
	}

	// Use a default scheme if none provided.
	if opts.scheme == nil {
		opts.scheme = runtime.NewScheme()
	}

	// Create the dynamic client.
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubeclient: failed to create dynamic client: %w", err)
	}

	// Use the provided RESTMapper, or fall back to a default.
	var mapper meta.RESTMapper
	if opts.mapper != nil {
		mapper = opts.mapper
	} else {
		mapper = meta.NewDefaultRESTMapper(nil)
	}

	// Create codec and store.
	c := codec.NewCodec(opts.scheme)
	s := store.NewDedupStore()

	// Create the cache.
	cacheInstance := cache.NewCache(dynClient, opts.scheme, opts.namespaces, opts.reconstructLRUSize)

	return &DedupClient{
		dynamic: dynClient,
		cache:   cacheInstance,
		codec:   c,
		store:   s,
		scheme:  opts.scheme,
		mapper:  mapper,
		opts:    opts,
	}, nil
}

// --- Reader methods (served from cache) ---

// Get retrieves a single object by key from the cache.
func (c *DedupClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.cache.Get(ctx, key, obj)
}

// List retrieves all objects of the given type from the cache.
func (c *DedupClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.cache.List(ctx, list, opts...)
}

// --- Writer methods (write-through: API server first, then cache) ---

// Create creates a new object on the API server and upserts it into the store.
func (c *DedupClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	unstrObj, err := c.codec.ToUnstructured(obj)
	if err != nil {
		return fmt.Errorf("kubeclient: Create: ToUnstructured failed: %w", err)
	}

	gvk, err := internal.GVKFromObject(obj, c.scheme)
	if err != nil {
		return fmt.Errorf("kubeclient: Create: cannot determine GVK: %w", err)
	}

	gvr := internal.GVRFromGVK(gvk)
	ns := obj.GetNamespace()

	var result interface{}
	if ns != "" {
		result, err = c.dynamic.Resource(gvr).Namespace(ns).Create(ctx, unstrObj, metav1.CreateOptions{})
	} else {
		result, err = c.dynamic.Resource(gvr).Create(ctx, unstrObj, metav1.CreateOptions{})
	}
	if err != nil {
		return fmt.Errorf("kubeclient: Create: API call failed: %w", err)
	}

	// result is *unstructured.Unstructured from the dynamic client.
	if resultUnstr, ok := result.(interface {
		GetNamespace() string
		GetName() string
	}); ok {
		storeKey := store.ObjectKey{
			GVK:       gvk,
			Namespace: resultUnstr.GetNamespace(),
			Name:      resultUnstr.GetName(),
		}
		if u, ok2 := result.(interface {
			GetObject() map[string]interface{}
		}); ok2 {
			_ = u
		}
		// Use codec to convert back — result is *unstructured.Unstructured.
		if unstrResult, ok2 := result.(client.Object); ok2 {
			unstrConverted, convErr := c.codec.ToUnstructured(unstrResult)
			if convErr == nil {
				_ = c.store.Upsert(storeKey, unstrConverted)
				return c.codec.FromUnstructured(unstrConverted, obj)
			}
		}
	}

	return nil
}

// Update updates an existing object on the API server and upserts it into the store.
func (c *DedupClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	unstrObj, err := c.codec.ToUnstructured(obj)
	if err != nil {
		return fmt.Errorf("kubeclient: Update: ToUnstructured failed: %w", err)
	}

	gvk, err := internal.GVKFromObject(obj, c.scheme)
	if err != nil {
		return fmt.Errorf("kubeclient: Update: cannot determine GVK: %w", err)
	}

	gvr := internal.GVRFromGVK(gvk)
	ns := obj.GetNamespace()

	var result interface{}
	if ns != "" {
		result, err = c.dynamic.Resource(gvr).Namespace(ns).Update(ctx, unstrObj, metav1.UpdateOptions{})
	} else {
		result, err = c.dynamic.Resource(gvr).Update(ctx, unstrObj, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("kubeclient: Update: API call failed: %w", err)
	}

	if unstrResult, ok := result.(client.Object); ok {
		unstrConverted, convErr := c.codec.ToUnstructured(unstrResult)
		if convErr == nil {
			storeKey := store.ObjectKey{
				GVK:       gvk,
				Namespace: unstrConverted.GetNamespace(),
				Name:      unstrConverted.GetName(),
			}
			_ = c.store.Upsert(storeKey, unstrConverted)
			return c.codec.FromUnstructured(unstrConverted, obj)
		}
	}

	return nil
}

// Patch applies a patch to an object on the API server and upserts it into the store.
func (c *DedupClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	gvk, err := internal.GVKFromObject(obj, c.scheme)
	if err != nil {
		return fmt.Errorf("kubeclient: Patch: cannot determine GVK: %w", err)
	}

	gvr := internal.GVRFromGVK(gvk)
	ns := obj.GetNamespace()
	name := obj.GetName()

	data, err := patch.Data(obj)
	if err != nil {
		return fmt.Errorf("kubeclient: Patch: failed to get patch data: %w", err)
	}

	patchType := patch.Type()

	var result interface{}
	if ns != "" {
		result, err = c.dynamic.Resource(gvr).Namespace(ns).Patch(ctx, name, patchType, data, metav1.PatchOptions{})
	} else {
		result, err = c.dynamic.Resource(gvr).Patch(ctx, name, patchType, data, metav1.PatchOptions{})
	}
	if err != nil {
		return fmt.Errorf("kubeclient: Patch: API call failed: %w", err)
	}

	if unstrResult, ok := result.(client.Object); ok {
		unstrConverted, convErr := c.codec.ToUnstructured(unstrResult)
		if convErr == nil {
			storeKey := store.ObjectKey{
				GVK:       gvk,
				Namespace: unstrConverted.GetNamespace(),
				Name:      unstrConverted.GetName(),
			}
			_ = c.store.Upsert(storeKey, unstrConverted)
			return c.codec.FromUnstructured(unstrConverted, obj)
		}
	}

	return nil
}

// Delete deletes an object from the API server and removes it from the store.
func (c *DedupClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	gvk, err := internal.GVKFromObject(obj, c.scheme)
	if err != nil {
		return fmt.Errorf("kubeclient: Delete: cannot determine GVK: %w", err)
	}

	gvr := internal.GVRFromGVK(gvk)
	ns := obj.GetNamespace()
	name := obj.GetName()

	if ns != "" {
		err = c.dynamic.Resource(gvr).Namespace(ns).Delete(ctx, name, metav1.DeleteOptions{})
	} else {
		err = c.dynamic.Resource(gvr).Delete(ctx, name, metav1.DeleteOptions{})
	}
	if err != nil {
		return fmt.Errorf("kubeclient: Delete: API call failed: %w", err)
	}

	storeKey := store.ObjectKey{
		GVK:       gvk,
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
	_ = c.store.Delete(storeKey)

	return nil
}

// DeleteAllOf deletes all objects of the given type matching the options.
func (c *DedupClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	gvk, err := internal.GVKFromObject(obj, c.scheme)
	if err != nil {
		return fmt.Errorf("kubeclient: DeleteAllOf: cannot determine GVK: %w", err)
	}

	gvr := internal.GVRFromGVK(gvk)

	// Parse options to get namespace.
	deleteAllOfOpts := &client.DeleteAllOfOptions{}
	for _, opt := range opts {
		opt.ApplyToDeleteAllOf(deleteAllOfOpts)
	}

	ns := deleteAllOfOpts.Namespace

	if ns != "" {
		err = c.dynamic.Resource(gvr).Namespace(ns).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
	} else {
		err = c.dynamic.Resource(gvr).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
	}
	if err != nil {
		return fmt.Errorf("kubeclient: DeleteAllOf: API call failed: %w", err)
	}

	// Remove all matching objects from the store.
	var storeOpts []store.ListOption
	if ns != "" {
		storeOpts = append(storeOpts, store.WithNamespace(ns))
	}
	items := c.store.List(gvk, storeOpts...)
	for _, item := range items {
		storeKey := store.ObjectKey{
			GVK:       gvk,
			Namespace: item.GetNamespace(),
			Name:      item.GetName(),
		}
		_ = c.store.Delete(storeKey)
	}

	return nil
}

// --- StatusClient ---

// Status returns a SubResourceWriter for the status subresource.
func (c *DedupClient) Status() client.SubResourceWriter {
	return &subResourceClient{
		client:      c,
		subResource: "status",
	}
}

// --- SubResourceClientConstructor ---

// SubResource returns a SubResourceClient for the given subresource.
func (c *DedupClient) SubResource(subResource string) client.SubResourceClient {
	return &subResourceClient{
		client:      c,
		subResource: subResource,
	}
}

// --- Scheme and RESTMapper ---

// Scheme returns the runtime.Scheme used by this client.
func (c *DedupClient) Scheme() *runtime.Scheme {
	return c.scheme
}

// RESTMapper returns the RESTMapper used by this client.
func (c *DedupClient) RESTMapper() meta.RESTMapper {
	return c.mapper
}

// GroupVersionKindFor returns the GVK for the given runtime.Object.
func (c *DedupClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	if clientObj, ok := obj.(client.Object); ok {
		gvk, err := c.codec.GVKForObject(clientObj)
		if err == nil {
			return gvk, nil
		}
	}

	gvks, _, err := c.scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("kubeclient: GroupVersionKindFor: %w", err)
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("kubeclient: GroupVersionKindFor: no GVK found")
	}
	return gvks[0], nil
}

// IsObjectNamespaced returns true if the given object is namespace-scoped.
func (c *DedupClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	gvk, err := c.GroupVersionKindFor(obj)
	if err != nil {
		return false, err
	}

	mappings, mapErr := c.mapper.RESTMappings(gvk.GroupKind(), gvk.Version)
	if mapErr == nil && len(mappings) > 0 {
		return mappings[0].Scope.Name() == meta.RESTScopeNameNamespace, nil
	}

	// Fall back: check if the object has a namespace set.
	if clientObj, ok := obj.(client.Object); ok {
		return clientObj.GetNamespace() != "", nil
	}

	return false, nil
}

// --- Cache management ---

// Cache returns the underlying cache for direct access.
func (c *DedupClient) Cache() *cache.Cache {
	return c.cache
}

// Start starts the cache informers. Blocks until ctx is cancelled.
func (c *DedupClient) Start(ctx context.Context) error {
	return c.cache.Start(ctx)
}

// WaitForCacheSync waits for all informers to sync.
func (c *DedupClient) WaitForCacheSync(ctx context.Context) bool {
	return c.cache.WaitForCacheSync(ctx)
}

// --- subResourceClient ---

// subResourceClient implements client.SubResourceClient for a specific subresource.
type subResourceClient struct {
	client      *DedupClient
	subResource string
}

// Compile-time interface checks.
var _ client.SubResourceClient = &subResourceClient{}
var _ client.SubResourceWriter = &subResourceClient{}

// Get retrieves the subresource of the given object.
func (s *subResourceClient) Get(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceGetOption) error {
	gvk, err := internal.GVKFromObject(obj, s.client.scheme)
	if err != nil {
		return fmt.Errorf("kubeclient: SubResource.Get: cannot determine GVK: %w", err)
	}

	gvr := internal.GVRFromGVK(gvk)
	ns := obj.GetNamespace()
	name := obj.GetName()

	var result interface{}
	if ns != "" {
		result, err = s.client.dynamic.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{}, s.subResource)
	} else {
		result, err = s.client.dynamic.Resource(gvr).Get(ctx, name, metav1.GetOptions{}, s.subResource)
	}
	if err != nil {
		return fmt.Errorf("kubeclient: SubResource.Get: API call failed: %w", err)
	}

	if unstrResult, ok := result.(client.Object); ok {
		unstrConverted, convErr := s.client.codec.ToUnstructured(unstrResult)
		if convErr == nil {
			return s.client.codec.FromUnstructured(unstrConverted, subResource)
		}
	}

	return nil
}

// Create creates the subresource for the given object.
func (s *subResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	gvk, err := internal.GVKFromObject(obj, s.client.scheme)
	if err != nil {
		return fmt.Errorf("kubeclient: SubResource.Create: cannot determine GVK: %w", err)
	}

	gvr := internal.GVRFromGVK(gvk)
	ns := obj.GetNamespace()

	unstrSub, err := s.client.codec.ToUnstructured(subResource)
	if err != nil {
		return fmt.Errorf("kubeclient: SubResource.Create: ToUnstructured failed: %w", err)
	}

	var result interface{}
	if ns != "" {
		result, err = s.client.dynamic.Resource(gvr).Namespace(ns).Apply(ctx, obj.GetName(), unstrSub, metav1.ApplyOptions{})
	} else {
		result, err = s.client.dynamic.Resource(gvr).Apply(ctx, obj.GetName(), unstrSub, metav1.ApplyOptions{})
	}
	if err != nil {
		return fmt.Errorf("kubeclient: SubResource.Create: API call failed: %w", err)
	}
	_ = result

	return nil
}

// Update updates the subresource for the given object.
func (s *subResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	gvk, err := internal.GVKFromObject(obj, s.client.scheme)
	if err != nil {
		return fmt.Errorf("kubeclient: SubResource.Update: cannot determine GVK: %w", err)
	}

	gvr := internal.GVRFromGVK(gvk)
	ns := obj.GetNamespace()

	// Check if a body override was provided via options.
	updateOpts := &client.SubResourceUpdateOptions{}
	for _, opt := range opts {
		opt.ApplyToSubResourceUpdate(updateOpts)
	}

	var updateObj client.Object
	if updateOpts.SubResourceBody != nil {
		updateObj = updateOpts.SubResourceBody
	} else {
		updateObj = obj
	}

	unstrObj, err := s.client.codec.ToUnstructured(updateObj)
	if err != nil {
		return fmt.Errorf("kubeclient: SubResource.Update: ToUnstructured failed: %w", err)
	}

	var result interface{}
	if s.subResource == "status" {
		if ns != "" {
			result, err = s.client.dynamic.Resource(gvr).Namespace(ns).UpdateStatus(ctx, unstrObj, metav1.UpdateOptions{})
		} else {
			result, err = s.client.dynamic.Resource(gvr).UpdateStatus(ctx, unstrObj, metav1.UpdateOptions{})
		}
	} else {
		if ns != "" {
			result, err = s.client.dynamic.Resource(gvr).Namespace(ns).Update(ctx, unstrObj, metav1.UpdateOptions{})
		} else {
			result, err = s.client.dynamic.Resource(gvr).Update(ctx, unstrObj, metav1.UpdateOptions{})
		}
	}
	if err != nil {
		return fmt.Errorf("kubeclient: SubResource.Update: API call failed: %w", err)
	}

	if unstrResult, ok := result.(client.Object); ok {
		unstrConverted, convErr := s.client.codec.ToUnstructured(unstrResult)
		if convErr == nil {
			storeKey := store.ObjectKey{
				GVK:       gvk,
				Namespace: unstrConverted.GetNamespace(),
				Name:      unstrConverted.GetName(),
			}
			_ = s.client.store.Upsert(storeKey, unstrConverted)
			return s.client.codec.FromUnstructured(unstrConverted, obj)
		}
	}

	return nil
}

// Patch applies a patch to the subresource of the given object.
func (s *subResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	gvk, err := internal.GVKFromObject(obj, s.client.scheme)
	if err != nil {
		return fmt.Errorf("kubeclient: SubResource.Patch: cannot determine GVK: %w", err)
	}

	gvr := internal.GVRFromGVK(gvk)
	ns := obj.GetNamespace()
	name := obj.GetName()

	// Check if a body override was provided via options.
	patchOpts := &client.SubResourcePatchOptions{}
	for _, opt := range opts {
		opt.ApplyToSubResourcePatch(patchOpts)
	}

	patchTarget := obj
	if patchOpts.SubResourceBody != nil {
		patchTarget = patchOpts.SubResourceBody
	}

	data, err := patch.Data(patchTarget)
	if err != nil {
		return fmt.Errorf("kubeclient: SubResource.Patch: failed to get patch data: %w", err)
	}

	patchType := patch.Type()

	var result interface{}
	if ns != "" {
		result, err = s.client.dynamic.Resource(gvr).Namespace(ns).Patch(ctx, name, patchType, data, metav1.PatchOptions{}, s.subResource)
	} else {
		result, err = s.client.dynamic.Resource(gvr).Patch(ctx, name, patchType, data, metav1.PatchOptions{}, s.subResource)
	}
	if err != nil {
		return fmt.Errorf("kubeclient: SubResource.Patch: API call failed: %w", err)
	}

	if unstrResult, ok := result.(client.Object); ok {
		unstrConverted, convErr := s.client.codec.ToUnstructured(unstrResult)
		if convErr == nil {
			storeKey := store.ObjectKey{
				GVK:       gvk,
				Namespace: unstrConverted.GetNamespace(),
				Name:      unstrConverted.GetName(),
			}
			_ = s.client.store.Upsert(storeKey, unstrConverted)
			return s.client.codec.FromUnstructured(unstrConverted, obj)
		}
	}

	return nil
}
