// Package kubeclient provides a Kubernetes client with a deduplicated cache.
// This file defines functional options for constructing a KubeClient instance.
package kubeclient

import (
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// options holds configuration for the DedupClient.
type options struct {
	scheme             *runtime.Scheme
	gcInterval         time.Duration
	namespaces         []string
	watchGVKs          []schema.GroupVersionKind
	mapper             meta.RESTMapper
	reconstructLRUSize int
}

// defaultOptions returns options with sensible defaults.
func defaultOptions() *options {
	return &options{
		gcInterval: 5 * time.Minute,
		namespaces: []string{}, // empty = all namespaces
	}
}

// Option is a functional option for DedupClient.
type Option func(*options)

// WithScheme sets the runtime.Scheme used for type conversion.
func WithScheme(s *runtime.Scheme) Option {
	return func(o *options) {
		o.scheme = s
	}
}

// WithGCInterval sets the interval between garbage collection runs.
func WithGCInterval(d time.Duration) Option {
	return func(o *options) {
		o.gcInterval = d
	}
}

// WithNamespaces restricts the cache to the given namespaces.
// If not set, all namespaces are watched.
func WithNamespaces(ns ...string) Option {
	return func(o *options) {
		o.namespaces = ns
	}
}

// WithGVKs pre-registers GVKs to watch on startup.
func WithGVKs(gvks ...schema.GroupVersionKind) Option {
	return func(o *options) {
		o.watchGVKs = append(o.watchGVKs, gvks...)
	}
}

// WithRESTMapper injects a custom RESTMapper into the client.
// If not set, meta.NewDefaultRESTMapper(nil) is used.
func WithRESTMapper(m meta.RESTMapper) Option {
	return func(o *options) {
		o.mapper = m
	}
}

// WithReconstructionCache enables an LRU cache for reconstructed objects.
// size is the maximum number of entries to keep in the cache.
func WithReconstructionCache(size int) Option {
	return func(o *options) {
		o.reconstructLRUSize = size
	}
}
