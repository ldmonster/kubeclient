// Package codec handles encoding and decoding between Kubernetes Unstructured
// objects and the internal node-tree representation used by the dedup store.
package codec

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Codec handles conversion between typed objects, Unstructured, and the internal node tree.
type Codec struct {
	scheme *runtime.Scheme
}

// NewCodec creates a new Codec with the given scheme.
func NewCodec(scheme *runtime.Scheme) *Codec {
	return &Codec{scheme: scheme}
}

// ToUnstructured converts a typed client.Object to *unstructured.Unstructured.
// If the object is already Unstructured, returns a deep copy.
func (c *Codec) ToUnstructured(obj client.Object) (*unstructured.Unstructured, error) {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u.DeepCopy(), nil
	}

	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, fmt.Errorf("codec: ToUnstructured failed: %w", err)
	}

	result := &unstructured.Unstructured{Object: m}

	// Ensure GVK is set on the result.
	gvk, err := c.GVKForObject(obj)
	if err == nil {
		c.SetGVK(result, gvk)
	}

	return result, nil
}

// FromUnstructured converts an *unstructured.Unstructured to a typed client.Object.
// The `into` parameter must be a pointer to the target typed struct.
// If `into` is an *unstructured.Unstructured, performs a deep copy.
func (c *Codec) FromUnstructured(u *unstructured.Unstructured, into client.Object) error {
	if target, ok := into.(*unstructured.Unstructured); ok {
		u.DeepCopyInto(target)
		return nil
	}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, into); err != nil {
		return fmt.Errorf("codec: FromUnstructured failed: %w", err)
	}
	return nil
}

// GVKForObject returns the GVK for the given object using the scheme.
func (c *Codec) GVKForObject(obj client.Object) (schema.GroupVersionKind, error) {
	gvks, _, err := c.scheme.ObjectKinds(obj)
	if err == nil {
		for _, gvk := range gvks {
			if gvk.Kind != "" {
				return gvk, nil
			}
		}
	}

	// Fall back to reading from the object's TypeMeta (e.g. for Unstructured).
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind != "" {
		return gvk, nil
	}

	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("codec: GVKForObject failed: %w", err)
	}
	return schema.GroupVersionKind{}, fmt.Errorf("codec: GVKForObject: no GVK found for object")
}

// SetGVK sets the TypeMeta (apiVersion + kind) on an Unstructured object from a GVK.
func (c *Codec) SetGVK(u *unstructured.Unstructured, gvk schema.GroupVersionKind) {
	u.SetGroupVersionKind(gvk)
}
