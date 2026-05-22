package store

import (
	"fmt"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var deploymentGVK = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
var podGVK = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

// labelsToInterface converts map[string]string to map[string]interface{}.
func labelsToInterface(labels map[string]string) map[string]interface{} {
	m := make(map[string]interface{}, len(labels))
	for k, v := range labels {
		m[k] = v
	}
	return m
}

func makeDeployment(name, namespace string, labels map[string]string, replicas int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
				"labels":    labelsToInterface(labels),
				"uid":       name + "-uid",
			},
			"spec": map[string]interface{}{
				"replicas": replicas,
				"selector": map[string]interface{}{
					"matchLabels": map[string]interface{}{
						"app": "myapp",
					},
				},
				"template": map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "myapp",
						},
					},
					"spec": map[string]interface{}{
						"containers": []interface{}{
							map[string]interface{}{
								"name":  "main",
								"image": "nginx:latest",
								"ports": []interface{}{
									map[string]interface{}{
										"containerPort": int64(80),
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestDedupStore_UpsertAndGet(t *testing.T) {
	s := NewDedupStore()
	obj := makeDeployment("deploy-1", "default", map[string]string{"app": "myapp"}, 3)
	key := ObjectKey{GVK: deploymentGVK, Namespace: "default", Name: "deploy-1"}

	if err := s.Upsert(key, obj); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	got, ok := s.Get(key)
	if !ok {
		t.Fatal("Get returned false after Upsert")
	}

	if got.GetName() != "deploy-1" {
		t.Errorf("Name = %q, want %q", got.GetName(), "deploy-1")
	}
	if got.GetNamespace() != "default" {
		t.Errorf("Namespace = %q, want %q", got.GetNamespace(), "default")
	}
	labels := got.GetLabels()
	if labels["app"] != "myapp" {
		t.Errorf("label app = %q, want %q", labels["app"], "myapp")
	}
}

func TestDedupStore_UpdateObject(t *testing.T) {
	s := NewDedupStore()
	key := ObjectKey{GVK: deploymentGVK, Namespace: "default", Name: "deploy-1"}

	obj1 := makeDeployment("deploy-1", "default", map[string]string{"app": "myapp"}, 1)
	if err := s.Upsert(key, obj1); err != nil {
		t.Fatalf("first Upsert failed: %v", err)
	}

	obj2 := makeDeployment("deploy-1", "default", map[string]string{"app": "myapp"}, 5)
	if err := s.Upsert(key, obj2); err != nil {
		t.Fatalf("second Upsert failed: %v", err)
	}

	got, ok := s.Get(key)
	if !ok {
		t.Fatal("Get returned false after update")
	}

	replicas, found, err := unstructured.NestedInt64(got.Object, "spec", "replicas")
	if err != nil || !found {
		t.Fatalf("could not read spec.replicas: err=%v found=%v", err, found)
	}
	if replicas != 5 {
		t.Errorf("spec.replicas = %d, want 5", replicas)
	}
}

func TestDedupStore_DeleteObject(t *testing.T) {
	s := NewDedupStore()
	key := ObjectKey{GVK: deploymentGVK, Namespace: "default", Name: "deploy-1"}
	obj := makeDeployment("deploy-1", "default", nil, 1)

	if err := s.Upsert(key, obj); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}
	if err := s.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, ok := s.Get(key)
	if ok {
		t.Fatal("Get returned true after Delete")
	}
}

func TestDedupStore_ListByGVK(t *testing.T) {
	s := NewDedupStore()

	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("deploy-%d", i)
		key := ObjectKey{GVK: deploymentGVK, Namespace: "default", Name: name}
		obj := makeDeployment(name, "default", nil, 1)
		if err := s.Upsert(key, obj); err != nil {
			t.Fatalf("Upsert deploy-%d failed: %v", i, err)
		}
	}

	for i := 0; i < 2; i++ {
		name := fmt.Sprintf("pod-%d", i)
		key := ObjectKey{GVK: podGVK, Namespace: "default", Name: name}
		obj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]interface{}{
					"name":      name,
					"namespace": "default",
				},
			},
		}
		if err := s.Upsert(key, obj); err != nil {
			t.Fatalf("Upsert pod-%d failed: %v", i, err)
		}
	}

	deployments := s.List(deploymentGVK)
	if len(deployments) != 3 {
		t.Errorf("List(deploymentGVK) returned %d objects, want 3", len(deployments))
	}

	pods := s.List(podGVK)
	if len(pods) != 2 {
		t.Errorf("List(podGVK) returned %d objects, want 2", len(pods))
	}
}

func TestDedupStore_ListWithNamespace(t *testing.T) {
	s := NewDedupStore()

	namespaces := []string{"ns-a", "ns-a", "ns-b"}
	for i, ns := range namespaces {
		name := fmt.Sprintf("deploy-%d", i)
		key := ObjectKey{GVK: deploymentGVK, Namespace: ns, Name: name}
		obj := makeDeployment(name, ns, nil, 1)
		if err := s.Upsert(key, obj); err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}
	}

	nsA := s.List(deploymentGVK, WithNamespace("ns-a"))
	if len(nsA) != 2 {
		t.Errorf("List with ns-a returned %d objects, want 2", len(nsA))
	}

	nsB := s.List(deploymentGVK, WithNamespace("ns-b"))
	if len(nsB) != 1 {
		t.Errorf("List with ns-b returned %d objects, want 1", len(nsB))
	}
}

func TestDedupStore_ListWithLabelSelector(t *testing.T) {
	s := NewDedupStore()

	objects := []struct {
		name   string
		labels map[string]string
	}{
		{"deploy-0", map[string]string{"env": "prod", "app": "web"}},
		{"deploy-1", map[string]string{"env": "prod", "app": "api"}},
		{"deploy-2", map[string]string{"env": "staging", "app": "web"}},
	}

	for _, o := range objects {
		key := ObjectKey{GVK: deploymentGVK, Namespace: "default", Name: o.name}
		obj := makeDeployment(o.name, "default", o.labels, 1)
		if err := s.Upsert(key, obj); err != nil {
			t.Fatalf("Upsert %s failed: %v", o.name, err)
		}
	}

	prodObjs := s.List(deploymentGVK, WithLabelSelector(map[string]string{"env": "prod"}))
	if len(prodObjs) != 2 {
		t.Errorf("List with env=prod returned %d objects, want 2", len(prodObjs))
	}

	prodWebObjs := s.List(deploymentGVK, WithLabelSelector(map[string]string{"env": "prod", "app": "web"}))
	if len(prodWebObjs) != 1 {
		t.Errorf("List with env=prod,app=web returned %d objects, want 1", len(prodWebObjs))
	}
}

func TestDedupStore_DeduplicationSavings(t *testing.T) {
	s := NewDedupStore()

	// Upsert 100 deployments with identical spec but different names.
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("deploy-%d", i)
		key := ObjectKey{GVK: deploymentGVK, Namespace: "default", Name: name}
		obj := makeDeployment(name, "default", map[string]string{"app": "myapp"}, 3)
		if err := s.Upsert(key, obj); err != nil {
			t.Fatalf("Upsert %s failed: %v", name, err)
		}
	}

	// The subtree pool should have far fewer nodes than 100 × (nodes per object).
	// A naive store would have 100 × N nodes; with dedup the spec subtree is shared.
	// We just verify the pool size is significantly less than 100 * 20 (a rough upper bound).
	poolLen := s.subtrees.Len()
	naiveUpperBound := 100 * 20
	if poolLen >= naiveUpperBound {
		t.Errorf("subtrees.Len() = %d, expected much less than %d (dedup not working)", poolLen, naiveUpperBound)
	}
	t.Logf("subtrees.Len() = %d (naive upper bound = %d)", poolLen, naiveUpperBound)
}

func TestDedupStore_GC(t *testing.T) {
	s := NewDedupStore()

	// Upsert some objects.
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("deploy-%d", i)
		key := ObjectKey{GVK: deploymentGVK, Namespace: "default", Name: name}
		obj := makeDeployment(name, "default", nil, 1)
		if err := s.Upsert(key, obj); err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}
	}

	// Delete all objects.
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("deploy-%d", i)
		key := ObjectKey{GVK: deploymentGVK, Namespace: "default", Name: name}
		if err := s.Delete(key); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	// Before GC, refcounts map may still have zero-count entries.
	stats := s.GC()
	t.Logf("GC removed %d nodes in %v", stats.NodesRemoved, stats.Duration)

	// After GC, no zero-count entries should remain in refcounts.
	s.mu.RLock()
	for id, count := range s.refcounts {
		if count == 0 {
			t.Errorf("refcounts[%d] = 0 after GC (should have been removed)", id)
		}
	}
	s.mu.RUnlock()
}

func TestDedupStore_Concurrent(t *testing.T) {
	s := NewDedupStore()
	const goroutines = 10
	const perGoroutine = 100

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				name := fmt.Sprintf("deploy-g%d-i%d", g, i)
				key := ObjectKey{GVK: deploymentGVK, Namespace: "default", Name: name}
				obj := makeDeployment(name, "default", map[string]string{"app": "myapp"}, int64(i))

				if err := s.Upsert(key, obj); err != nil {
					t.Errorf("Upsert %s failed: %v", name, err)
					return
				}

				if _, ok := s.Get(key); !ok {
					t.Errorf("Get %s returned false after Upsert", name)
					return
				}

				if err := s.Delete(key); err != nil {
					t.Errorf("Delete %s failed: %v", name, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
