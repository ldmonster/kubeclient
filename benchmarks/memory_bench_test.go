// Package benchmarks contains benchmarks comparing kubeclient's dedup store
// against a plain map (simulating go-client / controller-runtime in-memory cache).
package benchmarks

import (
	"fmt"
	"runtime"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/ldmonster/kubeclient/store"
)

// benchmarkMemoryDedupStore measures heap bytes consumed by storing n objects
// in the DedupStore.
func benchmarkMemoryDedupStore(b *testing.B, n int) {
	b.Helper()
	b.ReportAllocs()

	// Generate objects once outside the measurement window.
	pods := make([]*unstructured.Unstructured, n)
	for i := 0; i < n; i++ {
		pods[i] = makePod(
			fmt.Sprintf("pod-%d", i),
			fmt.Sprintf("ns-%d", i%10), // 10 namespaces — realistic spread
			i,
		)
	}

	b.ResetTimer()

	for range b.N {
		// Force a clean GC baseline before measuring.
		runtime.GC()
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		s := store.NewDedupStore()
		for _, pod := range pods {
			key := makeObjectKey(pod)
			if err := s.Upsert(key, pod); err != nil {
				b.Fatalf("Upsert failed: %v", err)
			}
		}

		runtime.GC()
		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)

		heapDelta := int64(memAfter.HeapInuse) - int64(memBefore.HeapInuse)
		if heapDelta < 0 {
			heapDelta = 0
		}
		b.ReportMetric(float64(heapDelta), "heap-bytes/op")
	}
}

// benchmarkMemoryPlainMap measures heap bytes consumed by storing n objects
// in a plain map[string]*unstructured.Unstructured (simulating go-client cache).
func benchmarkMemoryPlainMap(b *testing.B, n int) {
	b.Helper()
	b.ReportAllocs()

	// Generate objects once outside the measurement window.
	pods := make([]*unstructured.Unstructured, n)
	for i := 0; i < n; i++ {
		pods[i] = makePod(
			fmt.Sprintf("pod-%d", i),
			fmt.Sprintf("ns-%d", i%10),
			i,
		)
	}

	b.ResetTimer()

	for range b.N {
		runtime.GC()
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		m := make(map[string]*unstructured.Unstructured, n)
		for _, pod := range pods {
			mapKey := fmt.Sprintf("%s/%s/%s",
				pod.GetNamespace(),
				pod.GetName(),
				pod.GroupVersionKind().String(),
			)
			m[mapKey] = pod
		}

		runtime.GC()
		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)

		heapDelta := int64(memAfter.HeapInuse) - int64(memBefore.HeapInuse)
		if heapDelta < 0 {
			heapDelta = 0
		}
		b.ReportMetric(float64(heapDelta), "heap-bytes/op")

		// Keep m alive so the GC doesn't collect it before ReadMemStats.
		_ = m
	}
}

// --- DedupStore memory benchmarks ---

func BenchmarkMemory_DedupStore_100Objects(b *testing.B) {
	benchmarkMemoryDedupStore(b, 100)
}

func BenchmarkMemory_DedupStore_1000Objects(b *testing.B) {
	benchmarkMemoryDedupStore(b, 1000)
}

func BenchmarkMemory_DedupStore_10000Objects(b *testing.B) {
	benchmarkMemoryDedupStore(b, 10000)
}

// --- PlainMap memory benchmarks ---

func BenchmarkMemory_PlainMap_100Objects(b *testing.B) {
	benchmarkMemoryPlainMap(b, 100)
}

func BenchmarkMemory_PlainMap_1000Objects(b *testing.B) {
	benchmarkMemoryPlainMap(b, 1000)
}

func BenchmarkMemory_PlainMap_10000Objects(b *testing.B) {
	benchmarkMemoryPlainMap(b, 10000)
}
