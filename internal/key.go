// Package internal contains shared utilities used across KubeClient packages.
// This file provides object key utilities for namespace/name lookups.
package internal

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ObjectKeyFromObject extracts the namespace/name key from a client.Object.
func ObjectKeyFromObject(obj client.Object) client.ObjectKey {
	return client.ObjectKey{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
}

// GVKFromObject extracts the GVK from a client.Object using the provided scheme.
// Falls back to reading TypeMeta if scheme lookup fails.
func GVKFromObject(obj client.Object, scheme *runtime.Scheme) (schema.GroupVersionKind, error) {
	gvks, _, err := scheme.ObjectKinds(obj)
	if err == nil && len(gvks) > 0 {
		return gvks[0], nil
	}

	// Fall back to TypeMeta on the object itself.
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind != "" {
		return gvk, nil
	}

	return schema.GroupVersionKind{}, err
}
