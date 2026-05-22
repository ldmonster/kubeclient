# KubeClient

**KubeClient** is a Go library that provides a Kubernetes client with an intelligent, memory-efficient cache. It is fully compatible with the [`client.Client`](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/client#Client) interface from `sigs.k8s.io/controller-runtime`, making it a drop-in replacement for existing controllers and operators without any changes to calling code.

## Deduplicated Cache

The core innovation in KubeClient is a **deduplicated storage engine**. Rather than storing each Kubernetes object as an independent copy, the engine decomposes every `Unstructured` object into a tree of nodes and applies two levels of deduplication:

1. **Value interning** — scalar values (strings, integers, floats, booleans, `nil`) are stored once in a `ValueInternPool` and referenced by a compact `ValueID`. Field names such as `"metadata"`, `"spec"`, `"labels"`, common label values, and annotation keys are shared across every object in the cache.
2. **Subtree deduplication** — map and slice nodes are hashed and stored once in a `SubtreeInternPool`. Identical sub-objects (e.g., the same `securityContext`, `resources`, or `tolerations` block repeated across hundreds of Pods) occupy a single entry regardless of how many objects reference them.

For clusters with thousands of similar objects — such as Deployments generated from a common template — this approach can reduce cache memory usage by **60–90 %** compared to a standard informer cache.

## Features

- **Global deduplication** across all Kinds — identical field values and subtrees are stored once
- **Watch/Informer pattern** — cache is populated and kept up-to-date via the Kubernetes Watch API
- **Write-through cache** — `Create`, `Update`, and `Delete` operations update both the API server and the local cache atomically
- **Custom indexing** — efficient lookups by arbitrary object fields
- **Concurrent-safe** — all operations are safe for use from multiple goroutines
- **controller-runtime compatible** — implements the full `client.Client` interface

## Getting Started

```go
import "github.com/kubeclient/kubeclient"

client, err := kubeclient.New(cfg, kubeclient.Options{})
if err != nil {
    log.Fatal(err)
}
```

## License

Apache 2.0
