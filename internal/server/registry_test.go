package server

import (
	"reflect"
	"testing"
)

func TestConnRegistry(t *testing.T) {
	r := NewConnRegistry()
	if r.Count() != 0 {
		t.Fatalf("Count = %d, want 0", r.Count())
	}

	r.Add("192.168.1.10:5001")
	r.Add("192.168.1.20:5001")
	r.Add("192.168.1.30:5001")
	if r.Count() != 3 {
		t.Fatalf("Count = %d, want 3", r.Count())
	}

	// Add duplicate should not increase count.
	r.Add("192.168.1.10:5001")
	if r.Count() != 3 {
		t.Fatalf("Count after dup = %d, want 3", r.Count())
	}

	// List should be sorted.
	want := []string{"192.168.1.10:5001", "192.168.1.20:5001", "192.168.1.30:5001"}
	if got := r.List(); !reflect.DeepEqual(got, want) {
		t.Errorf("List = %v, want %v", got, want)
	}

	r.Remove("192.168.1.20:5001")
	if r.Count() != 2 {
		t.Errorf("Count after remove = %d, want 2", r.Count())
	}
	// Removing a non-existent entry should not panic or change count.
	r.Remove("9.9.9.9:1")
	if r.Count() != 2 {
		t.Errorf("Count after removing nonexistent = %d, want 2", r.Count())
	}
}
