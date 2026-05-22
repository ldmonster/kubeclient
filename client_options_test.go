package kubeclient

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestDefaultOptions_NonNilAndSensibleDefaults(t *testing.T) {
	opts := defaultOptions()
	if opts == nil {
		t.Fatal("defaultOptions() returned nil")
	}
	// scheme is nil by default (set later in New() if not provided).
	// gcInterval must be positive.
	if opts.gcInterval <= 0 {
		t.Errorf("defaultOptions().gcInterval = %v, want > 0", opts.gcInterval)
	}
	// namespaces should be non-nil and empty (watch all namespaces).
	if opts.namespaces == nil {
		t.Error("defaultOptions().namespaces is nil, want non-nil empty slice")
	}
	if len(opts.namespaces) != 0 {
		t.Errorf("defaultOptions().namespaces = %v, want empty slice", opts.namespaces)
	}
}

func TestDefaultOptions_GCInterval(t *testing.T) {
	opts := defaultOptions()
	want := 5 * time.Minute
	if opts.gcInterval != want {
		t.Errorf("defaultOptions().gcInterval = %v, want %v", opts.gcInterval, want)
	}
}

func TestWithScheme_SetsScheme(t *testing.T) {
	opts := defaultOptions()
	s := runtime.NewScheme()

	WithScheme(s)(opts)

	if opts.scheme != s {
		t.Errorf("WithScheme: opts.scheme = %p, want %p", opts.scheme, s)
	}
}

func TestWithScheme_OverridesExistingScheme(t *testing.T) {
	opts := defaultOptions()
	s1 := runtime.NewScheme()
	s2 := runtime.NewScheme()

	WithScheme(s1)(opts)
	WithScheme(s2)(opts)

	if opts.scheme != s2 {
		t.Error("WithScheme: second call should override the first")
	}
}

func TestWithGCInterval_SetsInterval(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
	}{
		{"1 minute", time.Minute},
		{"30 seconds", 30 * time.Second},
		{"10 minutes", 10 * time.Minute},
		{"zero", 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := defaultOptions()
			WithGCInterval(tc.d)(opts)
			if opts.gcInterval != tc.d {
				t.Errorf("WithGCInterval(%v): opts.gcInterval = %v, want %v", tc.d, opts.gcInterval, tc.d)
			}
		})
	}
}

func TestWithNamespaces_SetsNamespaces(t *testing.T) {
	tests := []struct {
		name       string
		namespaces []string
	}{
		{"single namespace", []string{"default"}},
		{"multiple namespaces", []string{"default", "kube-system", "production"}},
		{"empty list", []string{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := defaultOptions()
			WithNamespaces(tc.namespaces...)(opts)

			if len(opts.namespaces) != len(tc.namespaces) {
				t.Fatalf("WithNamespaces: len = %d, want %d", len(opts.namespaces), len(tc.namespaces))
			}
			for i, ns := range tc.namespaces {
				if opts.namespaces[i] != ns {
					t.Errorf("WithNamespaces: namespaces[%d] = %q, want %q", i, opts.namespaces[i], ns)
				}
			}
		})
	}
}

func TestWithGVKs_SetsGVKs(t *testing.T) {
	opts := defaultOptions()

	gvk1 := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	gvk2 := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

	WithGVKs(gvk1, gvk2)(opts)

	if len(opts.watchGVKs) != 2 {
		t.Fatalf("WithGVKs: len(watchGVKs) = %d, want 2", len(opts.watchGVKs))
	}
	if opts.watchGVKs[0] != gvk1 {
		t.Errorf("watchGVKs[0] = %v, want %v", opts.watchGVKs[0], gvk1)
	}
	if opts.watchGVKs[1] != gvk2 {
		t.Errorf("watchGVKs[1] = %v, want %v", opts.watchGVKs[1], gvk2)
	}
}

func TestWithGVKs_Appends(t *testing.T) {
	opts := defaultOptions()

	gvk1 := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	gvk2 := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

	WithGVKs(gvk1)(opts)
	WithGVKs(gvk2)(opts)

	if len(opts.watchGVKs) != 2 {
		t.Fatalf("WithGVKs called twice: len(watchGVKs) = %d, want 2", len(opts.watchGVKs))
	}
}

func TestWithGVKs_Empty(t *testing.T) {
	opts := defaultOptions()
	WithGVKs()(opts)

	if len(opts.watchGVKs) != 0 {
		t.Errorf("WithGVKs(): len(watchGVKs) = %d, want 0", len(opts.watchGVKs))
	}
}

func TestWithRESTMapper_SetsMapper(t *testing.T) {
	opts := defaultOptions()

	mapper := meta.NewDefaultRESTMapper(nil)
	WithRESTMapper(mapper)(opts)

	if opts.mapper != mapper {
		t.Error("WithRESTMapper: opts.mapper was not set correctly")
	}
}

func TestWithRESTMapper_NilMapper(t *testing.T) {
	opts := defaultOptions()
	WithRESTMapper(nil)(opts)

	if opts.mapper != nil {
		t.Error("WithRESTMapper(nil): opts.mapper should be nil")
	}
}

func TestWithReconstructionCache_SetsLRUSize(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"size 100", 100},
		{"size 1000", 1000},
		{"size 0", 0},
		{"size 1", 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := defaultOptions()
			WithReconstructionCache(tc.size)(opts)
			if opts.reconstructLRUSize != tc.size {
				t.Errorf("WithReconstructionCache(%d): opts.reconstructLRUSize = %d, want %d",
					tc.size, opts.reconstructLRUSize, tc.size)
			}
		})
	}
}

func TestWithReconstructionCache_DefaultIsZero(t *testing.T) {
	opts := defaultOptions()
	if opts.reconstructLRUSize != 0 {
		t.Errorf("default reconstructLRUSize = %d, want 0", opts.reconstructLRUSize)
	}
}

// TestOptions_CombinedOptions verifies that multiple options can be applied together.
func TestOptions_CombinedOptions(t *testing.T) {
	opts := defaultOptions()

	scheme := runtime.NewScheme()
	mapper := meta.NewDefaultRESTMapper(nil)
	gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

	WithScheme(scheme)(opts)
	WithGCInterval(2 * time.Minute)(opts)
	WithNamespaces("default", "staging")(opts)
	WithGVKs(gvk)(opts)
	WithRESTMapper(mapper)(opts)
	WithReconstructionCache(512)(opts)

	if opts.scheme != scheme {
		t.Error("scheme not set correctly")
	}
	if opts.gcInterval != 2*time.Minute {
		t.Errorf("gcInterval = %v, want 2m", opts.gcInterval)
	}
	if len(opts.namespaces) != 2 || opts.namespaces[0] != "default" || opts.namespaces[1] != "staging" {
		t.Errorf("namespaces = %v, want [default staging]", opts.namespaces)
	}
	if len(opts.watchGVKs) != 1 || opts.watchGVKs[0] != gvk {
		t.Errorf("watchGVKs = %v, want [%v]", opts.watchGVKs, gvk)
	}
	if opts.mapper != mapper {
		t.Error("mapper not set correctly")
	}
	if opts.reconstructLRUSize != 512 {
		t.Errorf("reconstructLRUSize = %d, want 512", opts.reconstructLRUSize)
	}
}
