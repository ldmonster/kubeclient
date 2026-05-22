package internal

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

// TestObjectKeyFromObject verifies that namespace and name are extracted correctly.
func TestObjectKeyFromObject(t *testing.T) {
	tests := []struct {
		name          string
		objName       string
		objNamespace  string
		wantName      string
		wantNamespace string
	}{
		{
			name:          "namespaced object",
			objName:       "my-pod",
			objNamespace:  "default",
			wantName:      "my-pod",
			wantNamespace: "default",
		},
		{
			name:          "cluster-scoped object (no namespace)",
			objName:       "my-node",
			objNamespace:  "",
			wantName:      "my-node",
			wantNamespace: "",
		},
		{
			name:          "empty name and namespace",
			objName:       "",
			objNamespace:  "",
			wantName:      "",
			wantNamespace: "",
		},
		{
			name:          "non-default namespace",
			objName:       "my-cm",
			objNamespace:  "kube-system",
			wantName:      "my-cm",
			wantNamespace: "kube-system",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{}
			pod.Name = tc.objName
			pod.Namespace = tc.objNamespace

			key := ObjectKeyFromObject(pod)
			if key.Name != tc.wantName {
				t.Errorf("ObjectKeyFromObject().Name = %q, want %q", key.Name, tc.wantName)
			}
			if key.Namespace != tc.wantNamespace {
				t.Errorf("ObjectKeyFromObject().Namespace = %q, want %q", key.Namespace, tc.wantNamespace)
			}
		})
	}
}

// TestObjectKeyFromObject_Unstructured verifies extraction from an Unstructured object.
func TestObjectKeyFromObject_Unstructured(t *testing.T) {
	u := &unstructured.Unstructured{}
	u.SetName("my-deploy")
	u.SetNamespace("production")

	key := ObjectKeyFromObject(u)
	if key.Name != "my-deploy" {
		t.Errorf("Name = %q, want %q", key.Name, "my-deploy")
	}
	if key.Namespace != "production" {
		t.Errorf("Namespace = %q, want %q", key.Namespace, "production")
	}
}

// TestGVKFromObject_SchemeRegistered verifies GVK lookup for types registered in the scheme.
func TestGVKFromObject_SchemeRegistered(t *testing.T) {
	// clientgoscheme.Scheme has core/v1 types registered.
	scheme := clientgoscheme.Scheme

	tests := []struct {
		name    string
		obj     runtime.Object
		wantGVK schema.GroupVersionKind
	}{
		{
			name:    "Pod",
			obj:     &corev1.Pod{},
			wantGVK: schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"},
		},
		{
			name:    "ConfigMap",
			obj:     &corev1.ConfigMap{},
			wantGVK: schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
		},
		{
			name:    "Service",
			obj:     &corev1.Service{},
			wantGVK: schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Service"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// GVKFromObject takes a client.Object; corev1 types implement it.
			obj, ok := tc.obj.(interface {
				GetNamespace() string
				GetName() string
				GetObjectKind() schema.ObjectKind
				DeepCopyObject() runtime.Object
			})
			if !ok {
				t.Fatalf("object does not implement client.Object")
			}
			_ = obj

			// Use the concrete corev1 types directly.
			switch v := tc.obj.(type) {
			case *corev1.Pod:
				gvk, err := GVKFromObject(v, scheme)
				if err != nil {
					t.Fatalf("GVKFromObject failed: %v", err)
				}
				if gvk != tc.wantGVK {
					t.Errorf("GVKFromObject() = %v, want %v", gvk, tc.wantGVK)
				}
			case *corev1.ConfigMap:
				gvk, err := GVKFromObject(v, scheme)
				if err != nil {
					t.Fatalf("GVKFromObject failed: %v", err)
				}
				if gvk != tc.wantGVK {
					t.Errorf("GVKFromObject() = %v, want %v", gvk, tc.wantGVK)
				}
			case *corev1.Service:
				gvk, err := GVKFromObject(v, scheme)
				if err != nil {
					t.Fatalf("GVKFromObject failed: %v", err)
				}
				if gvk != tc.wantGVK {
					t.Errorf("GVKFromObject() = %v, want %v", gvk, tc.wantGVK)
				}
			}
		})
	}
}

// TestGVKFromObject_Unstructured verifies GVK fallback via TypeMeta for Unstructured objects.
func TestGVKFromObject_Unstructured(t *testing.T) {
	scheme := runtime.NewScheme()

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	})

	gvk, err := GVKFromObject(u, scheme)
	if err != nil {
		t.Fatalf("GVKFromObject failed: %v", err)
	}
	want := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	if gvk != want {
		t.Errorf("GVKFromObject() = %v, want %v", gvk, want)
	}
}

// TestGVKFromObject_UnregisteredType verifies that an unregistered type falls back to TypeMeta.
func TestGVKFromObject_UnregisteredType(t *testing.T) {
	// Empty scheme — nothing registered.
	scheme := runtime.NewScheme()

	// An unstructured object with GVK set via TypeMeta should still work.
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "example.com",
		Version: "v1alpha1",
		Kind:    "MyResource",
	})

	gvk, err := GVKFromObject(u, scheme)
	if err != nil {
		t.Fatalf("GVKFromObject failed: %v", err)
	}
	if gvk.Kind != "MyResource" {
		t.Errorf("Kind = %q, want %q", gvk.Kind, "MyResource")
	}
	if gvk.Group != "example.com" {
		t.Errorf("Group = %q, want %q", gvk.Group, "example.com")
	}
	if gvk.Version != "v1alpha1" {
		t.Errorf("Version = %q, want %q", gvk.Version, "v1alpha1")
	}
}

// TestGVKFromObject_SchemePreferredOverTypeMeta verifies that scheme lookup takes
// precedence over TypeMeta when the type is registered.
func TestGVKFromObject_SchemePreferredOverTypeMeta(t *testing.T) {
	scheme := clientgoscheme.Scheme

	pod := &corev1.Pod{}
	// Set a wrong GVK on TypeMeta — scheme should win.
	pod.TypeMeta.Kind = "WrongKind"
	pod.TypeMeta.APIVersion = "wrong/v99"

	gvk, err := GVKFromObject(pod, scheme)
	if err != nil {
		t.Fatalf("GVKFromObject failed: %v", err)
	}
	if gvk.Kind != "Pod" {
		t.Errorf("Kind = %q, want %q (scheme should override TypeMeta)", gvk.Kind, "Pod")
	}
}
