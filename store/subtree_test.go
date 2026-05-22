package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestSubtreeInternPool_ScalarDedup(t *testing.T) {
	p := NewSubtreeInternPool()
	vp := NewValueInternPool()

	vid := vp.Intern("hello")
	node1 := Node{Kind: NodeKindScalar, ValueID: vid}
	node2 := Node{Kind: NodeKindScalar, ValueID: vid}

	id1 := p.Intern(node1)
	id2 := p.Intern(node2)

	if id1 != id2 {
		t.Fatalf("identical scalar nodes got different NodeIDs: %d vs %d", id1, id2)
	}
}

func TestSubtreeInternPool_MapDedup(t *testing.T) {
	p := NewSubtreeInternPool()
	vp := NewValueInternPool()

	keyID := vp.Intern("name")
	valNode := Node{Kind: NodeKindScalar, ValueID: vp.Intern("alice")}
	valID := p.Intern(valNode)

	entries := []MapEntry{{Key: keyID, Value: valID}}
	map1 := Node{Kind: NodeKindMap, MapEntries: entries}
	map2 := Node{Kind: NodeKindMap, MapEntries: []MapEntry{{Key: keyID, Value: valID}}}

	id1 := p.Intern(map1)
	id2 := p.Intern(map2)

	if id1 != id2 {
		t.Fatalf("identical map nodes got different NodeIDs: %d vs %d", id1, id2)
	}
}

func TestSubtreeInternPool_SliceDedup(t *testing.T) {
	p := NewSubtreeInternPool()
	vp := NewValueInternPool()

	item1ID := p.Intern(Node{Kind: NodeKindScalar, ValueID: vp.Intern("a")})
	item2ID := p.Intern(Node{Kind: NodeKindScalar, ValueID: vp.Intern("b")})

	slice1 := Node{Kind: NodeKindSlice, SliceItems: []NodeID{item1ID, item2ID}}
	slice2 := Node{Kind: NodeKindSlice, SliceItems: []NodeID{item1ID, item2ID}}

	id1 := p.Intern(slice1)
	id2 := p.Intern(slice2)

	if id1 != id2 {
		t.Fatalf("identical slice nodes got different NodeIDs: %d vs %d", id1, id2)
	}
}

func TestSubtreeInternPool_DifferentNodes(t *testing.T) {
	p := NewSubtreeInternPool()
	vp := NewValueInternPool()

	nodeA := Node{Kind: NodeKindScalar, ValueID: vp.Intern("foo")}
	nodeB := Node{Kind: NodeKindScalar, ValueID: vp.Intern("bar")}

	idA := p.Intern(nodeA)
	idB := p.Intern(nodeB)

	if idA == idB {
		t.Fatalf("different nodes got same NodeID: %d", idA)
	}

	// A scalar and a map with the same ValueID should differ.
	nodeMap := Node{Kind: NodeKindMap, MapEntries: []MapEntry{}}
	idMap := p.Intern(nodeMap)
	if idA == idMap {
		t.Fatalf("scalar and map nodes got same NodeID: %d", idA)
	}
}

func TestSubtreeInternPool_NestedDedup(t *testing.T) {
	p := NewSubtreeInternPool()
	vp := NewValueInternPool()

	// Build: map{ "items": slice[ scalar("x"), scalar("y") ] }
	buildNested := func() NodeID {
		scalarX := p.Intern(Node{Kind: NodeKindScalar, ValueID: vp.Intern("x")})
		scalarY := p.Intern(Node{Kind: NodeKindScalar, ValueID: vp.Intern("y")})
		sliceNode := p.Intern(Node{Kind: NodeKindSlice, SliceItems: []NodeID{scalarX, scalarY}})
		keyID := vp.Intern("items")
		return p.Intern(Node{Kind: NodeKindMap, MapEntries: []MapEntry{{Key: keyID, Value: sliceNode}}})
	}

	id1 := buildNested()
	id2 := buildNested()

	if id1 != id2 {
		t.Fatalf("identical nested structures got different NodeIDs: %d vs %d", id1, id2)
	}

	// Verify the inner nodes are also shared (pool Len should not double).
	lenAfterFirst := p.Len()
	// Building again should not add new nodes.
	id3 := buildNested()
	if id3 != id1 {
		t.Fatalf("third build got different NodeID: %d vs %d", id3, id1)
	}
	if p.Len() != lenAfterFirst {
		t.Fatalf("Len changed after re-interning identical nested structure: %d vs %d", p.Len(), lenAfterFirst)
	}
}

func TestSubtreeInternPool_Concurrent(t *testing.T) {
	p := NewSubtreeInternPool()
	vp := NewValueInternPool()

	const goroutines = 10
	const perGoroutine = 100

	var wg sync.WaitGroup
	ids := make([][]NodeID, goroutines)
	for g := range ids {
		ids[g] = make([]NodeID, perGoroutine)
	}

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				vid := vp.Intern(fmt.Sprintf("val-%d", i))
				node := Node{Kind: NodeKindScalar, ValueID: vid}
				ids[g][i] = p.Intern(node)
			}
		}(g)
	}
	wg.Wait()

	// All goroutines must agree on the same NodeID for the same content.
	for i := 0; i < perGoroutine; i++ {
		expected := ids[0][i]
		for g := 1; g < goroutines; g++ {
			if ids[g][i] != expected {
				t.Errorf("goroutine %d: val-%d got NodeID %d, want %d", g, i, ids[g][i], expected)
			}
		}
	}
}
