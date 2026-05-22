// Package store implements the deduplicated storage engine for KubeClient.
// This file defines the DedupStore interface — the primary contract for
// storing and retrieving Kubernetes Unstructured objects.
package store

import (
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ObjectKey uniquely identifies a Kubernetes object in the store.
type ObjectKey struct {
	GVK       schema.GroupVersionKind
	Namespace string
	Name      string
}

// listOptions holds the parsed options for a List query.
type listOptions struct {
	labelSelector  map[string]string
	fieldSelectors map[string]string
	namespace      string
}

// ListOption is a functional option for List queries.
type ListOption func(*listOptions)

// WithLabelSelector filters List results by label selector.
func WithLabelSelector(labels map[string]string) ListOption {
	return func(o *listOptions) {
		o.labelSelector = labels
	}
}

// WithNamespace filters List results by namespace.
func WithNamespace(ns string) ListOption {
	return func(o *listOptions) {
		o.namespace = ns
	}
}

// WithFieldSelector filters List results by a dot-notation field path and value.
// For example, WithFieldSelector("spec.nodeName", "node-1") matches objects
// where obj.spec.nodeName == "node-1".
func WithFieldSelector(field, value string) ListOption {
	return func(o *listOptions) {
		if o.fieldSelectors == nil {
			o.fieldSelectors = make(map[string]string)
		}
		o.fieldSelectors[field] = value
	}
}

// GCStats contains statistics from a GC run.
type GCStats struct {
	NodesRemoved  int
	ValuesRemoved int
	Duration      time.Duration
}

// Store is the interface for the deduplicated object store.
type Store interface {
	// Upsert adds or updates an object in the store.
	Upsert(key ObjectKey, obj *unstructured.Unstructured) error

	// Delete removes an object from the store.
	Delete(key ObjectKey) error

	// Get retrieves an object from the store.
	Get(key ObjectKey) (*unstructured.Unstructured, bool)

	// List returns all objects matching the given GVK and options.
	List(gvk schema.GroupVersionKind, opts ...ListOption) []*unstructured.Unstructured

	// GC runs garbage collection, removing unreferenced nodes.
	GC() GCStats
}
