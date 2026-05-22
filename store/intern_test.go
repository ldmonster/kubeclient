package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestValueInternPool_NilSentinel(t *testing.T) {
	p := NewValueInternPool()

	id := p.Intern(nil)
	if id != ValueID(0) {
		t.Fatalf("Intern(nil) = %d, want 0", id)
	}

	val := p.Resolve(0)
	if val != nil {
		t.Fatalf("Resolve(0) = %v, want nil", val)
	}
}

func TestValueInternPool_StringInterning(t *testing.T) {
	p := NewValueInternPool()

	id1 := p.Intern("hello")
	id2 := p.Intern("hello")
	if id1 != id2 {
		t.Fatalf("same string got different IDs: %d vs %d", id1, id2)
	}

	id3 := p.Intern("world")
	if id1 == id3 {
		t.Fatalf("different strings got same ID: %d", id1)
	}

	// Verify resolve works
	if got := p.Resolve(id1); got != "hello" {
		t.Fatalf("Resolve(%d) = %v, want %q", id1, got, "hello")
	}
	if got := p.Resolve(id3); got != "world" {
		t.Fatalf("Resolve(%d) = %v, want %q", id3, got, "world")
	}
}

func TestValueInternPool_NumericNormalization(t *testing.T) {
	p := NewValueInternPool()

	idInt := p.Intern(int(42))
	idInt32 := p.Intern(int32(42))
	idInt64 := p.Intern(int64(42))
	idUint := p.Intern(uint(42))

	if idInt != idInt32 {
		t.Errorf("int(42) and int32(42) got different IDs: %d vs %d", idInt, idInt32)
	}
	if idInt != idInt64 {
		t.Errorf("int(42) and int64(42) got different IDs: %d vs %d", idInt, idInt64)
	}
	if idInt != idUint {
		t.Errorf("int(42) and uint(42) got different IDs: %d vs %d", idInt, idUint)
	}

	// Verify the resolved value is int64(42)
	resolved := p.Resolve(idInt)
	if resolved != int64(42) {
		t.Errorf("Resolve(%d) = %v (%T), want int64(42)", idInt, resolved, resolved)
	}
}

func TestValueInternPool_BoolInterning(t *testing.T) {
	p := NewValueInternPool()

	idTrue1 := p.Intern(true)
	idTrue2 := p.Intern(true)
	idFalse1 := p.Intern(false)
	idFalse2 := p.Intern(false)

	if idTrue1 != idTrue2 {
		t.Errorf("true interned twice got different IDs: %d vs %d", idTrue1, idTrue2)
	}
	if idFalse1 != idFalse2 {
		t.Errorf("false interned twice got different IDs: %d vs %d", idFalse1, idFalse2)
	}
	if idTrue1 == idFalse1 {
		t.Errorf("true and false got same ID: %d", idTrue1)
	}
}

func TestValueInternPool_Concurrent(t *testing.T) {
	p := NewValueInternPool()
	const goroutines = 10
	const perGoroutine = 1000

	var wg sync.WaitGroup
	// ids[g][i] will hold the ID returned by goroutine g for string i
	ids := make([][]ValueID, goroutines)
	for g := range ids {
		ids[g] = make([]ValueID, perGoroutine)
	}

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				s := fmt.Sprintf("key-%d", i)
				ids[g][i] = p.Intern(s)
			}
		}(g)
	}
	wg.Wait()

	// All goroutines must have gotten the same ID for the same string.
	for i := 0; i < perGoroutine; i++ {
		expected := ids[0][i]
		for g := 1; g < goroutines; g++ {
			if ids[g][i] != expected {
				t.Errorf("goroutine %d: key-%d got ID %d, want %d", g, i, ids[g][i], expected)
			}
		}
	}
}

func TestValueInternPool_Len(t *testing.T) {
	p := NewValueInternPool()

	// Initially only the nil sentinel (index 0)
	if got := p.Len(); got != 1 {
		t.Fatalf("initial Len() = %d, want 1", got)
	}

	p.Intern("a")
	if got := p.Len(); got != 2 {
		t.Fatalf("after 1 intern Len() = %d, want 2", got)
	}

	p.Intern("b")
	p.Intern("c")
	if got := p.Len(); got != 4 {
		t.Fatalf("after 3 interns Len() = %d, want 4", got)
	}

	// Interning duplicates must not increase Len.
	p.Intern("a")
	p.Intern("b")
	if got := p.Len(); got != 4 {
		t.Fatalf("after duplicate interns Len() = %d, want 4", got)
	}
}
