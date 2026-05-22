// Package benchmarks contains benchmarks comparing kubeclient's dedup store
// against a plain map (simulating go-client / controller-runtime in-memory cache).
package benchmarks

import (
	"fmt"
	"runtime"
	"testing"

	k8scache "k8s.io/client-go/tools/cache"
)

// benchmarkMemoryCtrlRuntime measures heap bytes consumed by storing n typed
// *corev1.Pod objects in a k8scache.Indexer — the exact data structure that
// controller-runtime uses internally to back its in-memory cache.
func benchmarkMemoryCtrlRuntime(b *testing.B, n int) {
	b.Helper()
	b.ReportAllocs()

	// Generate typed pod objects once outside the measurement window.
	pods := make([]interface{}, n)
	for i := 0; i < n; i++ {
		pods[i] = makePodTyped(
			fmt.Sprintf("pod-%d", i),
			fmt.Sprintf("ns-%d", i%10),
		)
	}

	b.ResetTimer()

	for range b.N {
		// Force a clean GC baseline before measuring.
		runtime.GC()
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		indexer := k8scache.NewIndexer(
			k8scache.MetaNamespaceKeyFunc,
			k8scache.Indexers{},
		)
		for _, pod := range pods {
			if err := indexer.Add(pod); err != nil {
				b.Fatalf("indexer.Add failed: %v", err)
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

		// Keep indexer alive so the GC doesn't collect it before ReadMemStats.
		_ = indexer
	}
}

// --- CtrlRuntime memory benchmarks ---

func BenchmarkMemory_CtrlRuntime_100Objects(b *testing.B) {
	benchmarkMemoryCtrlRuntime(b, 100)
}

func BenchmarkMemory_CtrlRuntime_1000Objects(b *testing.B) {
	benchmarkMemoryCtrlRuntime(b, 1000)
}

func BenchmarkMemory_CtrlRuntime_10000Objects(b *testing.B) {
	benchmarkMemoryCtrlRuntime(b, 10000)
}

// --- CtrlRuntime throughput benchmarks ---

// BenchmarkThroughput_CtrlRuntime_Add measures the throughput of Add
// on a pre-populated k8scache.Indexer (controller-runtime's backing store).
func BenchmarkThroughput_CtrlRuntime_Add(b *testing.B) {
	indexer := k8scache.NewIndexer(
		k8scache.MetaNamespaceKeyFunc,
		k8scache.Indexers{},
	)

	// Pre-populate with prePopulateN typed pod objects.
	pods := make([]interface{}, prePopulateN)
	for i := 0; i < prePopulateN; i++ {
		pods[i] = makePodTyped(fmt.Sprintf("pod-%d", i), fmt.Sprintf("ns-%d", i%10))
		if err := indexer.Add(pods[i]); err != nil {
			b.Fatalf("pre-populate Add failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := indexer.Add(pods[i%prePopulateN]); err != nil {
			b.Fatalf("Add failed: %v", err)
		}
	}
}

// BenchmarkThroughput_CtrlRuntime_Get measures the throughput of GetByKey
// on a pre-populated k8scache.Indexer.
func BenchmarkThroughput_CtrlRuntime_Get(b *testing.B) {
	indexer := k8scache.NewIndexer(
		k8scache.MetaNamespaceKeyFunc,
		k8scache.Indexers{},
	)

	keys := make([]string, prePopulateN)
	for i := 0; i < prePopulateN; i++ {
		name := fmt.Sprintf("pod-%d", i)
		ns := fmt.Sprintf("ns-%d", i%10)
		pod := makePodTyped(name, ns)
		if err := indexer.Add(pod); err != nil {
			b.Fatalf("pre-populate Add failed: %v", err)
		}
		keys[i] = fmt.Sprintf("%s/%s", ns, name)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _ = indexer.GetByKey(keys[i%prePopulateN])
	}
}

// BenchmarkThroughput_CtrlRuntime_List measures the throughput of List
// on a pre-populated k8scache.Indexer.
func BenchmarkThroughput_CtrlRuntime_List(b *testing.B) {
	indexer := k8scache.NewIndexer(
		k8scache.MetaNamespaceKeyFunc,
		k8scache.Indexers{},
	)

	for i := 0; i < prePopulateN; i++ {
		pod := makePodTyped(fmt.Sprintf("pod-%d", i), fmt.Sprintf("ns-%d", i%10))
		if err := indexer.Add(pod); err != nil {
			b.Fatalf("pre-populate Add failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = indexer.List()
	}
}
