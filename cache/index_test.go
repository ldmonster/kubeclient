package cache

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ldmonster/kubeclient/store"
)

var testDeploymentGVK = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
var testPodGVK = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

// makeUnstructured creates a simple unstructured object for testing.
func makeUnstructured(gvk schema.GroupVersionKind, name, namespace string, labels map[string]string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gvk.GroupVersion().String(),
			"kind":       gvk.Kind,
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
		},
	}
	if len(labels) > 0 {
		labelMap := make(map[string]interface{}, len(labels))
		for k, v := range labels {
			labelMap[k] = v
		}
		obj.Object["metadata"].(map[string]interface{})["labels"] = labelMap
	}
	return obj
}

// namespaceIndexFunc is a simple IndexerFunc that returns the object's namespace.
func namespaceIndexFunc(obj client.Object) []string {
	return []string{obj.GetNamespace()}
}

// labelIndexFunc returns the value of a specific label.
func labelIndexFunc(labelKey string) IndexerFunc {
	return func(obj client.Object) []string {
		labels := obj.GetLabels()
		if v, ok := labels[labelKey]; ok {
			return []string{v}
		}
		return nil
	}
}

// multiValueIndexFunc returns multiple index values.
func multiValueIndexFunc(obj client.Object) []string {
	labels := obj.GetLabels()
	result := make([]string, 0, len(labels))
	for k, v := range labels {
		result = append(result, k+"="+v)
	}
	return result
}

func TestIndexer_AddIndex(t *testing.T) {
	idx := NewIndexer()

	// First registration should succeed.
	err := idx.AddIndex(testDeploymentGVK, "namespace", namespaceIndexFunc)
	if err != nil {
		t.Fatalf("first AddIndex failed: %v", err)
	}

	// Second registration of the same GVK+field should fail.
	err = idx.AddIndex(testDeploymentGVK, "namespace", namespaceIndexFunc)
	if err == nil {
		t.Fatal("second AddIndex for same GVK+field should return error, got nil")
	}
}

func TestIndexer_Lookup_Empty(t *testing.T) {
	idx := NewIndexer()

	// Lookup on a non-existent index should return nil/empty.
	keys := idx.Lookup(testDeploymentGVK, "namespace", "default")
	if len(keys) != 0 {
		t.Errorf("Lookup on non-existent index returned %d keys, want 0", len(keys))
	}
}

func TestIndexer_UpdateAndLookup(t *testing.T) {
	idx := NewIndexer()

	if err := idx.AddIndex(testDeploymentGVK, "namespace", namespaceIndexFunc); err != nil {
		t.Fatalf("AddIndex failed: %v", err)
	}

	obj := makeUnstructured(testDeploymentGVK, "deploy-1", "production", nil)
	key := store.ObjectKey{GVK: testDeploymentGVK, Namespace: "production", Name: "deploy-1"}
	idx.UpdateIndex(key, obj)

	keys := idx.Lookup(testDeploymentGVK, "namespace", "production")
	if len(keys) != 1 {
		t.Fatalf("Lookup returned %d keys, want 1", len(keys))
	}
	if keys[0] != key {
		t.Errorf("Lookup returned key %v, want %v", keys[0], key)
	}

	// Lookup for a different value should return nothing.
	keys2 := idx.Lookup(testDeploymentGVK, "namespace", "staging")
	if len(keys2) != 0 {
		t.Errorf("Lookup for staging returned %d keys, want 0", len(keys2))
	}
}

func TestIndexer_UpdateIndex_MultipleValues(t *testing.T) {
	idx := NewIndexer()

	if err := idx.AddIndex(testDeploymentGVK, "labels", multiValueIndexFunc); err != nil {
		t.Fatalf("AddIndex failed: %v", err)
	}

	obj := makeUnstructured(testDeploymentGVK, "deploy-1", "default", map[string]string{
		"env": "prod",
		"app": "web",
	})
	key := store.ObjectKey{GVK: testDeploymentGVK, Namespace: "default", Name: "deploy-1"}
	idx.UpdateIndex(key, obj)

	// Both label values should be indexed.
	envKeys := idx.Lookup(testDeploymentGVK, "labels", "env=prod")
	if len(envKeys) != 1 {
		t.Errorf("Lookup env=prod returned %d keys, want 1", len(envKeys))
	}

	appKeys := idx.Lookup(testDeploymentGVK, "labels", "app=web")
	if len(appKeys) != 1 {
		t.Errorf("Lookup app=web returned %d keys, want 1", len(appKeys))
	}
}

func TestIndexer_RemoveIndex(t *testing.T) {
	idx := NewIndexer()

	if err := idx.AddIndex(testDeploymentGVK, "namespace", namespaceIndexFunc); err != nil {
		t.Fatalf("AddIndex failed: %v", err)
	}

	obj := makeUnstructured(testDeploymentGVK, "deploy-1", "default", nil)
	key := store.ObjectKey{GVK: testDeploymentGVK, Namespace: "default", Name: "deploy-1"}
	idx.UpdateIndex(key, obj)

	// Verify it's indexed.
	if keys := idx.Lookup(testDeploymentGVK, "namespace", "default"); len(keys) != 1 {
		t.Fatalf("before RemoveIndex: Lookup returned %d keys, want 1", len(keys))
	}

	idx.RemoveIndex(key)

	// After removal, lookup should return empty.
	keys := idx.Lookup(testDeploymentGVK, "namespace", "default")
	if len(keys) != 0 {
		t.Errorf("after RemoveIndex: Lookup returned %d keys, want 0", len(keys))
	}
}

func TestIndexer_UpdateIndex_Idempotent(t *testing.T) {
	idx := NewIndexer()

	if err := idx.AddIndex(testDeploymentGVK, "namespace", namespaceIndexFunc); err != nil {
		t.Fatalf("AddIndex failed: %v", err)
	}

	obj := makeUnstructured(testDeploymentGVK, "deploy-1", "default", nil)
	key := store.ObjectKey{GVK: testDeploymentGVK, Namespace: "default", Name: "deploy-1"}

	// Update the same object twice.
	idx.UpdateIndex(key, obj)
	idx.UpdateIndex(key, obj)

	// Should still return exactly one key, not two.
	keys := idx.Lookup(testDeploymentGVK, "namespace", "default")
	if len(keys) != 1 {
		t.Errorf("after two UpdateIndex calls: Lookup returned %d keys, want 1", len(keys))
	}
}

func TestIndexer_MultipleGVKs(t *testing.T) {
	idx := NewIndexer()

	if err := idx.AddIndex(testDeploymentGVK, "namespace", namespaceIndexFunc); err != nil {
		t.Fatalf("AddIndex for Deployment failed: %v", err)
	}
	if err := idx.AddIndex(testPodGVK, "namespace", namespaceIndexFunc); err != nil {
		t.Fatalf("AddIndex for Pod failed: %v", err)
	}

	deployObj := makeUnstructured(testDeploymentGVK, "deploy-1", "ns-a", nil)
	deployKey := store.ObjectKey{GVK: testDeploymentGVK, Namespace: "ns-a", Name: "deploy-1"}
	idx.UpdateIndex(deployKey, deployObj)

	podObj := makeUnstructured(testPodGVK, "pod-1", "ns-b", nil)
	podKey := store.ObjectKey{GVK: testPodGVK, Namespace: "ns-b", Name: "pod-1"}
	idx.UpdateIndex(podKey, podObj)

	// Deployment index should only contain the deployment.
	deployKeys := idx.Lookup(testDeploymentGVK, "namespace", "ns-a")
	if len(deployKeys) != 1 {
		t.Errorf("Deployment Lookup ns-a returned %d keys, want 1", len(deployKeys))
	}

	// Pod index should only contain the pod.
	podKeys := idx.Lookup(testPodGVK, "namespace", "ns-b")
	if len(podKeys) != 1 {
		t.Errorf("Pod Lookup ns-b returned %d keys, want 1", len(podKeys))
	}

	// Cross-GVK lookups should return nothing.
	crossKeys := idx.Lookup(testDeploymentGVK, "namespace", "ns-b")
	if len(crossKeys) != 0 {
		t.Errorf("Deployment Lookup ns-b returned %d keys, want 0", len(crossKeys))
	}

	crossKeys2 := idx.Lookup(testPodGVK, "namespace", "ns-a")
	if len(crossKeys2) != 0 {
		t.Errorf("Pod Lookup ns-a returned %d keys, want 0", len(crossKeys2))
	}
}
