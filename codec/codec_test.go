package codec

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	return s
}

func newTestCodec() *Codec {
	return NewCodec(newTestScheme())
}

func TestCodec_ToUnstructured_AlreadyUnstructured(t *testing.T) {
	c := newTestCodec()

	original := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name":      "test-cm",
				"namespace": "default",
			},
			"data": map[string]interface{}{
				"key": "value",
			},
		},
	}

	result, err := c.ToUnstructured(original)
	if err != nil {
		t.Fatalf("ToUnstructured failed: %v", err)
	}

	// Must be a deep copy, not the same pointer.
	if result == original {
		t.Fatal("ToUnstructured returned the same pointer, expected a deep copy")
	}

	// Content must match.
	if result.GetName() != original.GetName() {
		t.Errorf("Name = %q, want %q", result.GetName(), original.GetName())
	}
	if result.GetNamespace() != original.GetNamespace() {
		t.Errorf("Namespace = %q, want %q", result.GetNamespace(), original.GetNamespace())
	}

	// Mutating the copy must not affect the original.
	result.SetName("mutated")
	if original.GetName() != "test-cm" {
		t.Error("mutating the copy affected the original (not a deep copy)")
	}
}

func TestCodec_ToUnstructured_TypedObject(t *testing.T) {
	c := newTestCodec()

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-config",
			Namespace: "kube-system",
		},
		Data: map[string]string{
			"foo": "bar",
		},
	}

	result, err := c.ToUnstructured(cm)
	if err != nil {
		t.Fatalf("ToUnstructured failed: %v", err)
	}

	if result.GetName() != "my-config" {
		t.Errorf("Name = %q, want %q", result.GetName(), "my-config")
	}
	if result.GetNamespace() != "kube-system" {
		t.Errorf("Namespace = %q, want %q", result.GetNamespace(), "kube-system")
	}

	data, found, err := unstructured.NestedStringMap(result.Object, "data")
	if err != nil || !found {
		t.Fatalf("data field not found: err=%v found=%v", err, found)
	}
	if data["foo"] != "bar" {
		t.Errorf("data[foo] = %q, want %q", data["foo"], "bar")
	}
}

func TestCodec_FromUnstructured_ToTyped(t *testing.T) {
	c := newTestCodec()

	u := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name":      "round-trip",
				"namespace": "default",
			},
			"data": map[string]interface{}{
				"hello": "world",
			},
		},
	}

	var cm corev1.ConfigMap
	if err := c.FromUnstructured(u, &cm); err != nil {
		t.Fatalf("FromUnstructured failed: %v", err)
	}

	if cm.Name != "round-trip" {
		t.Errorf("Name = %q, want %q", cm.Name, "round-trip")
	}
	if cm.Namespace != "default" {
		t.Errorf("Namespace = %q, want %q", cm.Namespace, "default")
	}
	if cm.Data["hello"] != "world" {
		t.Errorf("Data[hello] = %q, want %q", cm.Data["hello"], "world")
	}
}

func TestCodec_FromUnstructured_ToUnstructured(t *testing.T) {
	c := newTestCodec()

	original := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name": "copy-test",
			},
		},
	}

	target := &unstructured.Unstructured{}
	if err := c.FromUnstructured(original, target); err != nil {
		t.Fatalf("FromUnstructured failed: %v", err)
	}

	if target.GetName() != "copy-test" {
		t.Errorf("Name = %q, want %q", target.GetName(), "copy-test")
	}

	// Must be a deep copy.
	target.SetName("mutated")
	if original.GetName() != "copy-test" {
		t.Error("mutating target affected original (not a deep copy)")
	}
}

func TestCodec_GVKForObject_Typed(t *testing.T) {
	c := newTestCodec()

	cm := &corev1.ConfigMap{}
	gvk, err := c.GVKForObject(cm)
	if err != nil {
		t.Fatalf("GVKForObject failed: %v", err)
	}

	if gvk.Group != "" {
		t.Errorf("Group = %q, want %q", gvk.Group, "")
	}
	if gvk.Version != "v1" {
		t.Errorf("Version = %q, want %q", gvk.Version, "v1")
	}
	if gvk.Kind != "ConfigMap" {
		t.Errorf("Kind = %q, want %q", gvk.Kind, "ConfigMap")
	}
}

func TestCodec_GVKForObject_Unstructured(t *testing.T) {
	c := newTestCodec()

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	})

	gvk, err := c.GVKForObject(u)
	if err != nil {
		t.Fatalf("GVKForObject failed: %v", err)
	}

	if gvk.Group != "apps" {
		t.Errorf("Group = %q, want %q", gvk.Group, "apps")
	}
	if gvk.Version != "v1" {
		t.Errorf("Version = %q, want %q", gvk.Version, "v1")
	}
	if gvk.Kind != "Deployment" {
		t.Errorf("Kind = %q, want %q", gvk.Kind, "Deployment")
	}
}

func TestCodec_SetGVK(t *testing.T) {
	c := newTestCodec()

	u := &unstructured.Unstructured{Object: map[string]interface{}{}}
	gvk := schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"}
	c.SetGVK(u, gvk)

	got := u.GetObjectKind().GroupVersionKind()
	if got != gvk {
		t.Errorf("GVK = %v, want %v", got, gvk)
	}

	// Verify apiVersion and kind fields directly.
	apiVersion, _, _ := unstructured.NestedString(u.Object, "apiVersion")
	kind, _, _ := unstructured.NestedString(u.Object, "kind")

	if apiVersion != "batch/v1" {
		t.Errorf("apiVersion = %q, want %q", apiVersion, "batch/v1")
	}
	if kind != "Job" {
		t.Errorf("kind = %q, want %q", kind, "Job")
	}
}
