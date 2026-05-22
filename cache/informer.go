// Package cache implements the caching layer for KubeClient.
// This file manages the informer lifecycle — starting, stopping, and
// synchronising Watch/List operations against the Kubernetes API server.
package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ldmonster/kubeclient/store"
)

// EventHandler is called when objects are added, updated, or deleted.
type EventHandler interface {
	OnAdd(obj client.Object)
	OnUpdate(oldObj, newObj client.Object)
	OnDelete(obj client.Object)
}

// GVKInformer watches a single GVK and feeds events into the DedupStore.
// It performs an initial List to populate the store, then watches for changes.
type GVKInformer struct {
	mu        sync.RWMutex
	gvk       schema.GroupVersionKind
	gvr       schema.GroupVersionResource
	dynamic   dynamic.Interface
	store     store.Store
	indexer   *Indexer
	handlers  []EventHandler
	hasSynced bool
	namespace string // empty = all namespaces
}

// NewGVKInformer creates a new GVKInformer.
func NewGVKInformer(
	gvk schema.GroupVersionKind,
	gvr schema.GroupVersionResource,
	dynamicClient dynamic.Interface,
	s store.Store,
	indexer *Indexer,
	namespace string,
) *GVKInformer {
	return &GVKInformer{
		gvk:       gvk,
		gvr:       gvr,
		dynamic:   dynamicClient,
		store:     s,
		indexer:   indexer,
		namespace: namespace,
	}
}

// AddEventHandler registers an event handler.
func (inf *GVKInformer) AddEventHandler(handler EventHandler) {
	inf.mu.Lock()
	defer inf.mu.Unlock()
	inf.handlers = append(inf.handlers, handler)
}

// HasSynced returns true if the initial List has completed.
func (inf *GVKInformer) HasSynced() bool {
	inf.mu.RLock()
	defer inf.mu.RUnlock()
	return inf.hasSynced
}

// Run starts the informer. It performs an initial List, then watches.
// Blocks until ctx is cancelled. Implements reconnect with exponential backoff.
func (inf *GVKInformer) Run(ctx context.Context) error {
	// Initial list to populate the store.
	resourceVersion, err := inf.list(ctx)
	if err != nil {
		return fmt.Errorf("informer: initial list failed for %v: %w", inf.gvk, err)
	}

	inf.mu.Lock()
	inf.hasSynced = true
	inf.mu.Unlock()

	// Watch loop with exponential backoff on error.
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := inf.watchLoop(ctx, resourceVersion)
		if err == nil {
			// ctx was cancelled.
			return ctx.Err()
		}

		// Check if context was cancelled.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Backoff before reconnecting.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}

		// Re-list to get a fresh resourceVersion after reconnect.
		resourceVersion, err = inf.list(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		} else {
			// Reset backoff on successful list.
			backoff = time.Second
		}
	}
}

// list performs the initial List and populates the store.
func (inf *GVKInformer) list(ctx context.Context) (string, error) {
	var listResult *unstructured.UnstructuredList
	var err error

	ri := inf.dynamic.Resource(inf.gvr)
	if inf.namespace != "" {
		listResult, err = ri.Namespace(inf.namespace).List(ctx, metav1.ListOptions{})
	} else {
		listResult, err = ri.List(ctx, metav1.ListOptions{})
	}
	if err != nil {
		return "", fmt.Errorf("informer: list failed: %w", err)
	}

	for i := range listResult.Items {
		item := &listResult.Items[i]
		key := store.ObjectKey{
			GVK:       inf.gvk,
			Namespace: item.GetNamespace(),
			Name:      item.GetName(),
		}
		if err := inf.store.Upsert(key, item); err != nil {
			return "", fmt.Errorf("informer: store upsert failed: %w", err)
		}
		inf.indexer.UpdateIndex(key, item)
	}

	return listResult.GetResourceVersion(), nil
}

// watchLoop starts a Watch from the given resourceVersion and processes events.
func (inf *GVKInformer) watchLoop(ctx context.Context, rv string) error {
	var watcher watch.Interface
	var err error

	ri := inf.dynamic.Resource(inf.gvr)
	if inf.namespace != "" {
		watcher, err = ri.Namespace(inf.namespace).Watch(ctx, metav1.ListOptions{
			ResourceVersion: rv,
		})
	} else {
		watcher, err = ri.Watch(ctx, metav1.ListOptions{
			ResourceVersion: rv,
		})
	}
	if err != nil {
		return fmt.Errorf("informer: watch failed: %w", err)
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("informer: watch channel closed")
			}
			if err := inf.handleEvent(event); err != nil {
				return err
			}
		}
	}
}

// handleEvent processes a single watch event.
func (inf *GVKInformer) handleEvent(event watch.Event) error {
	switch event.Type {
	case watch.Bookmark:
		// Update resourceVersion from bookmark — nothing to do here since
		// we pass rv into watchLoop; the caller will re-list on reconnect.
		return nil

	case watch.Error:
		return fmt.Errorf("informer: received error event")

	case watch.Added, watch.Modified:
		u, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			return fmt.Errorf("informer: unexpected object type %T", event.Object)
		}

		key := store.ObjectKey{
			GVK:       inf.gvk,
			Namespace: u.GetNamespace(),
			Name:      u.GetName(),
		}

		// Get old object for OnUpdate (may not exist for Added).
		var oldObj client.Object
		if event.Type == watch.Modified {
			if existing, found := inf.store.Get(key); found {
				oldObj = existing
			}
		}

		if err := inf.store.Upsert(key, u); err != nil {
			return fmt.Errorf("informer: store upsert failed: %w", err)
		}
		inf.indexer.UpdateIndex(key, u)

		inf.mu.RLock()
		handlers := inf.handlers
		inf.mu.RUnlock()

		for _, h := range handlers {
			if event.Type == watch.Added {
				h.OnAdd(u)
			} else {
				if oldObj != nil {
					h.OnUpdate(oldObj, u)
				} else {
					h.OnAdd(u)
				}
			}
		}

	case watch.Deleted:
		u, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			return fmt.Errorf("informer: unexpected object type %T", event.Object)
		}

		key := store.ObjectKey{
			GVK:       inf.gvk,
			Namespace: u.GetNamespace(),
			Name:      u.GetName(),
		}

		if err := inf.store.Delete(key); err != nil {
			return fmt.Errorf("informer: store delete failed: %w", err)
		}
		inf.indexer.RemoveIndex(key)

		inf.mu.RLock()
		handlers := inf.handlers
		inf.mu.RUnlock()

		for _, h := range handlers {
			h.OnDelete(u)
		}
	}

	return nil
}
