package store

import "testing"

// TestNodeKind_Constants verifies the iota values for NodeKind constants.
func TestNodeKind_Constants(t *testing.T) {
	tests := []struct {
		name string
		got  NodeKind
		want NodeKind
	}{
		{"NodeKindScalar", NodeKindScalar, 0},
		{"NodeKindMap", NodeKindMap, 1},
		{"NodeKindSlice", NodeKindSlice, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
			}
		})
	}
}

// TestNode_ZeroValue verifies that a zero-value Node has NodeKindScalar and zero IDs.
func TestNode_ZeroValue(t *testing.T) {
	var n Node
	if n.Kind != NodeKindScalar {
		t.Errorf("zero Node.Kind = %d, want NodeKindScalar (%d)", n.Kind, NodeKindScalar)
	}
	if n.ValueID != 0 {
		t.Errorf("zero Node.ValueID = %d, want 0", n.ValueID)
	}
	if len(n.MapEntries) != 0 {
		t.Errorf("zero Node.MapEntries len = %d, want 0", len(n.MapEntries))
	}
	if len(n.SliceItems) != 0 {
		t.Errorf("zero Node.SliceItems len = %d, want 0", len(n.SliceItems))
	}
}

// TestNode_ScalarCreation verifies creating a scalar Node.
func TestNode_ScalarCreation(t *testing.T) {
	n := Node{
		Kind:    NodeKindScalar,
		ValueID: ValueID(42),
	}
	if n.Kind != NodeKindScalar {
		t.Errorf("Kind = %d, want NodeKindScalar", n.Kind)
	}
	if n.ValueID != 42 {
		t.Errorf("ValueID = %d, want 42", n.ValueID)
	}
	if len(n.MapEntries) != 0 {
		t.Errorf("MapEntries should be empty for scalar, got len %d", len(n.MapEntries))
	}
	if len(n.SliceItems) != 0 {
		t.Errorf("SliceItems should be empty for scalar, got len %d", len(n.SliceItems))
	}
}

// TestNode_MapCreation verifies creating a map Node with MapEntries.
func TestNode_MapCreation(t *testing.T) {
	entries := []MapEntry{
		{Key: ValueID(1), Value: NodeID(10)},
		{Key: ValueID(2), Value: NodeID(20)},
	}
	n := Node{
		Kind:       NodeKindMap,
		MapEntries: entries,
	}
	if n.Kind != NodeKindMap {
		t.Errorf("Kind = %d, want NodeKindMap", n.Kind)
	}
	if len(n.MapEntries) != 2 {
		t.Fatalf("MapEntries len = %d, want 2", len(n.MapEntries))
	}
	if n.MapEntries[0].Key != 1 || n.MapEntries[0].Value != 10 {
		t.Errorf("MapEntries[0] = {%d, %d}, want {1, 10}", n.MapEntries[0].Key, n.MapEntries[0].Value)
	}
	if n.MapEntries[1].Key != 2 || n.MapEntries[1].Value != 20 {
		t.Errorf("MapEntries[1] = {%d, %d}, want {2, 20}", n.MapEntries[1].Key, n.MapEntries[1].Value)
	}
	if n.ValueID != 0 {
		t.Errorf("ValueID should be 0 for map node, got %d", n.ValueID)
	}
}

// TestNode_SliceCreation verifies creating a slice Node with SliceItems.
func TestNode_SliceCreation(t *testing.T) {
	items := []NodeID{NodeID(5), NodeID(10), NodeID(15)}
	n := Node{
		Kind:       NodeKindSlice,
		SliceItems: items,
	}
	if n.Kind != NodeKindSlice {
		t.Errorf("Kind = %d, want NodeKindSlice", n.Kind)
	}
	if len(n.SliceItems) != 3 {
		t.Fatalf("SliceItems len = %d, want 3", len(n.SliceItems))
	}
	for i, want := range []NodeID{5, 10, 15} {
		if n.SliceItems[i] != want {
			t.Errorf("SliceItems[%d] = %d, want %d", i, n.SliceItems[i], want)
		}
	}
	if n.ValueID != 0 {
		t.Errorf("ValueID should be 0 for slice node, got %d", n.ValueID)
	}
}

// TestMapEntry_Creation verifies MapEntry struct field assignment.
func TestMapEntry_Creation(t *testing.T) {
	e := MapEntry{Key: ValueID(7), Value: NodeID(99)}
	if e.Key != 7 {
		t.Errorf("MapEntry.Key = %d, want 7", e.Key)
	}
	if e.Value != 99 {
		t.Errorf("MapEntry.Value = %d, want 99", e.Value)
	}
}

// TestMapEntry_ZeroValue verifies zero-value MapEntry.
func TestMapEntry_ZeroValue(t *testing.T) {
	var e MapEntry
	if e.Key != 0 {
		t.Errorf("zero MapEntry.Key = %d, want 0", e.Key)
	}
	if e.Value != 0 {
		t.Errorf("zero MapEntry.Value = %d, want 0", e.Value)
	}
}

// TestNodeID_ValueID_Types verifies that NodeID and ValueID are distinct uint64-based types.
func TestNodeID_ValueID_Types(t *testing.T) {
	var nid NodeID = 100
	var vid ValueID = 200

	if uint64(nid) != 100 {
		t.Errorf("NodeID(100) as uint64 = %d, want 100", uint64(nid))
	}
	if uint64(vid) != 200 {
		t.Errorf("ValueID(200) as uint64 = %d, want 200", uint64(vid))
	}
}

// TestNode_EmptyMapNode verifies a map node with no entries.
func TestNode_EmptyMapNode(t *testing.T) {
	n := Node{
		Kind:       NodeKindMap,
		MapEntries: []MapEntry{},
	}
	if n.Kind != NodeKindMap {
		t.Errorf("Kind = %d, want NodeKindMap", n.Kind)
	}
	if len(n.MapEntries) != 0 {
		t.Errorf("MapEntries len = %d, want 0", len(n.MapEntries))
	}
}

// TestNode_EmptySliceNode verifies a slice node with no items.
func TestNode_EmptySliceNode(t *testing.T) {
	n := Node{
		Kind:       NodeKindSlice,
		SliceItems: []NodeID{},
	}
	if n.Kind != NodeKindSlice {
		t.Errorf("Kind = %d, want NodeKindSlice", n.Kind)
	}
	if len(n.SliceItems) != 0 {
		t.Errorf("SliceItems len = %d, want 0", len(n.SliceItems))
	}
}
