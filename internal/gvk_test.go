package internal

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestGVKToString(t *testing.T) {
	tests := []struct {
		name string
		gvk  schema.GroupVersionKind
		want string
	}{
		{
			name: "non-empty group",
			gvk:  schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			want: "apps/v1/Deployment",
		},
		{
			name: "empty group (core API)",
			gvk:  schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"},
			want: "v1/Pod",
		},
		{
			name: "batch group",
			gvk:  schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"},
			want: "batch/v1/Job",
		},
		{
			name: "empty group with empty kind",
			gvk:  schema.GroupVersionKind{Group: "", Version: "v1", Kind: ""},
			want: "v1/",
		},
		{
			name: "all empty",
			gvk:  schema.GroupVersionKind{},
			want: "/",
		},
		{
			name: "custom resource group",
			gvk:  schema.GroupVersionKind{Group: "example.com", Version: "v1alpha1", Kind: "MyResource"},
			want: "example.com/v1alpha1/MyResource",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := GVKToString(tc.gvk)
			if got != tc.want {
				t.Errorf("GVKToString(%v) = %q, want %q", tc.gvk, got, tc.want)
			}
		})
	}
}

func TestGVRFromGVK(t *testing.T) {
	tests := []struct {
		name         string
		gvk          schema.GroupVersionKind
		wantGroup    string
		wantVersion  string
		wantResource string
	}{
		{
			name:         "Deployment",
			gvk:          schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
			wantGroup:    "apps",
			wantVersion:  "v1",
			wantResource: "deployments",
		},
		{
			name:         "Pod (core API)",
			gvk:          schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"},
			wantGroup:    "",
			wantVersion:  "v1",
			wantResource: "pods",
		},
		{
			name:         "Service",
			gvk:          schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Service"},
			wantGroup:    "",
			wantVersion:  "v1",
			wantResource: "services",
		},
		{
			name:         "ConfigMap",
			gvk:          schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
			wantGroup:    "",
			wantVersion:  "v1",
			wantResource: "configmaps",
		},
		{
			name:         "uppercase kind is lowercased",
			gvk:          schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"},
			wantGroup:    "batch",
			wantVersion:  "v1",
			wantResource: "jobs",
		},
		{
			name:         "custom resource",
			gvk:          schema.GroupVersionKind{Group: "example.com", Version: "v1alpha1", Kind: "MyResource"},
			wantGroup:    "example.com",
			wantVersion:  "v1alpha1",
			wantResource: "myresources",
		},
		{
			name:         "empty kind produces just 's'",
			gvk:          schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: ""},
			wantGroup:    "apps",
			wantVersion:  "v1",
			wantResource: "s",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := GVRFromGVK(tc.gvk)
			if got.Group != tc.wantGroup {
				t.Errorf("GVRFromGVK(%v).Group = %q, want %q", tc.gvk, got.Group, tc.wantGroup)
			}
			if got.Version != tc.wantVersion {
				t.Errorf("GVRFromGVK(%v).Version = %q, want %q", tc.gvk, got.Version, tc.wantVersion)
			}
			if got.Resource != tc.wantResource {
				t.Errorf("GVRFromGVK(%v).Resource = %q, want %q", tc.gvk, got.Resource, tc.wantResource)
			}
		})
	}
}

// TestGVRFromGVK_GroupVersionPreserved verifies that Group and Version are
// passed through unchanged (not lowercased or modified).
func TestGVRFromGVK_GroupVersionPreserved(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "Apps", Version: "V1", Kind: "Deployment"}
	got := GVRFromGVK(gvk)
	if got.Group != "Apps" {
		t.Errorf("Group = %q, want %q (group should not be modified)", got.Group, "Apps")
	}
	if got.Version != "V1" {
		t.Errorf("Version = %q, want %q (version should not be modified)", got.Version, "V1")
	}
	// Only Kind is lowercased.
	if got.Resource != "deployments" {
		t.Errorf("Resource = %q, want %q", got.Resource, "deployments")
	}
}
