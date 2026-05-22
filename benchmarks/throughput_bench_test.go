// Package benchmarks contains benchmarks comparing kubeclient's dedup store
// against a plain map (simulating go-client / controller-runtime in-memory cache).
package benchmarks

import (
	"fmt"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/ldmonster/kubeclient/store"
)

const prePopulateN = 1000

// --- DedupStore throughput benchmarks ---

// BenchmarkThroughput_DedupStore_Upsert measures the throughput of Upsert
// on a pre-populated DedupStore.
func BenchmarkThroughput_DedupStore_Upsert(b *testing.B) {
	s := store.NewDedupStore()

	// Pre-populate with prePopulateN objects.
	pods := make([]*unstructured.Unstructured, prePopulateN)
	for i := 0; i < prePopulateN; i++ {
		pods[i] = makePod(fmt.Sprintf("pod-%d", i), fmt.Sprintf("ns-%d", i%10), i)
		if err := s.Upsert(makeObjectKey(pods[i]), pods[i]); err != nil {
			b.Fatalf("pre-populate Upsert failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pod := pods[i%prePopulateN]
		if err := s.Upsert(makeObjectKey(pod), pod); err != nil {
			b.Fatalf("Upsert failed: %v", err)
		}
	}
}

// BenchmarkThroughput_DedupStore_Get measures the throughput of Get
// on a pre-populated DedupStore.
func BenchmarkThroughput_DedupStore_Get(b *testing.B) {
	s := store.NewDedupStore()

	pods := make([]*unstructured.Unstructured, prePopulateN)
	keys := make([]store.ObjectKey, prePopulateN)
	for i := 0; i < prePopulateN; i++ {
		pods[i] = makePod(fmt.Sprintf("pod-%d", i), fmt.Sprintf("ns-%d", i%10), i)
		keys[i] = makeObjectKey(pods[i])
		if err := s.Upsert(keys[i], pods[i]); err != nil {
			b.Fatalf("pre-populate Upsert failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = s.Get(keys[i%prePopulateN])
	}
}

// BenchmarkThroughput_DedupStore_List measures the throughput of List
// on a pre-populated DedupStore.
func BenchmarkThroughput_DedupStore_List(b *testing.B) {
	s := store.NewDedupStore()

	for i := 0; i < prePopulateN; i++ {
		pod := makePod(fmt.Sprintf("pod-%d", i), fmt.Sprintf("ns-%d", i%10), i)
		if err := s.Upsert(makeObjectKey(pod), pod); err != nil {
			b.Fatalf("pre-populate Upsert failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = s.List(podGVK)
	}
}

// --- PlainMap throughput benchmarks ---

// plainMap is a simple thread-safe map[string]*unstructured.Unstructured
// that simulates what go-client / controller-runtime use internally.
type plainMap struct {
	mu    sync.RWMutex
	items map[string]*unstructured.Unstructured
}

func newPlainMap(capacity int) *plainMap {
	return &plainMap{items: make(map[string]*unstructured.Unstructured, capacity)}
}

func (m *plainMap) key(u *unstructured.Unstructured) string {
	return fmt.Sprintf("%s/%s/%s", u.GetNamespace(), u.GetName(), u.GroupVersionKind().String())
}

func (m *plainMap) upsert(u *unstructured.Unstructured) {
	m.mu.Lock()
	m.items[m.key(u)] = u
	m.mu.Unlock()
}

func (m *plainMap) get(u *unstructured.Unstructured) (*unstructured.Unstructured, bool) {
	m.mu.RLock()
	v, ok := m.items[m.key(u)]
	m.mu.RUnlock()
	return v, ok
}

func (m *plainMap) list() []*unstructured.Unstructured {
	m.mu.RLock()
	result := make([]*unstructured.Unstructured, 0, len(m.items))
	for _, v := range m.items {
		result = append(result, v)
	}
	m.mu.RUnlock()
	return result
}

// BenchmarkThroughput_PlainMap_Upsert measures the throughput of map writes.
func BenchmarkThroughput_PlainMap_Upsert(b *testing.B) {
	m := newPlainMap(prePopulateN)

	pods := make([]*unstructured.Unstructured, prePopulateN)
	for i := 0; i < prePopulateN; i++ {
		pods[i] = makePod(fmt.Sprintf("pod-%d", i), fmt.Sprintf("ns-%d", i%10), i)
		m.upsert(pods[i])
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		m.upsert(pods[i%prePopulateN])
	}
}

// BenchmarkThroughput_PlainMap_Get measures the throughput of map reads.
func BenchmarkThroughput_PlainMap_Get(b *testing.B) {
	m := newPlainMap(prePopulateN)

	pods := make([]*unstructured.Unstructured, prePopulateN)
	for i := 0; i < prePopulateN; i++ {
		pods[i] = makePod(fmt.Sprintf("pod-%d", i), fmt.Sprintf("ns-%d", i%10), i)
		m.upsert(pods[i])
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = m.get(pods[i%prePopulateN])
	}
}

// BenchmarkThroughput_PlainMap_List measures the throughput of iterating all map entries.
func BenchmarkThroughput_PlainMap_List(b *testing.B) {
	m := newPlainMap(prePopulateN)

	for i := 0; i < prePopulateN; i++ {
		pod := makePod(fmt.Sprintf("pod-%d", i), fmt.Sprintf("ns-%d", i%10), i)
		m.upsert(pod)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = m.list()
	}
}
