package store

import (
	"testing"

	"github.com/ssenthilnathan3/kvgo/internal/persistence"
)

type mockPersister struct {
    data map[string]persistence.Entry
}

func (m *mockPersister) Load() (map[string]persistence.Entry, error) {
    return m.data, nil
}

func (m *mockPersister) Save(data map[string]persistence.Entry) error {
    m.data = data
    return nil
}

type mockWAL struct{}

func (m *mockWAL) AppendLog(timestamp, command, key, value string) error {
    return nil
}

func TestPutAndGet(t *testing.T) {
	s := &Store{
		Data: make(map[string]persistence.Entry),
		Persister: &mockPersister{
			data: make(map[string]persistence.Entry),
		},
		WAL: &persistence.WALLoader{
			WALPath: "/dev/null",
		},
	}

	err := s.Put("key1", "value1")
	if err != nil {
		t.Fatalf("Error adding key %v", err)
	}

	val, err := s.Get("key1")
	if err != nil {
		t.Fatalf("Error getting key %v", err)
	}

	if val != "value1" {
		t.Fatalf("The value recieved for the key %s is not correct: recieved => %s", "key1", val)
	}
}

func TestGetMissingKey(t *testing.T) {
	s := &Store{
		Data: make(map[string]persistence.Entry),
		Persister: &mockPersister{
			data: make(map[string]persistence.Entry),
		},
		WAL: &persistence.WALLoader{
			WALPath: "/dev/null",
		},
	}

	_, err := s.Get("nonexistent")
	if err == nil {
		t.Fatal("Expected error for nonexistent key")
	}
}

func TestDeleteKey(t *testing.T) {
	s := &Store{
		Data: make(map[string]persistence.Entry),
		Persister: &mockPersister{
			data: make(map[string]persistence.Entry),
		},
		WAL: &persistence.WALLoader{
			WALPath: "/dev/null",
		},
	}

	err := s.Put("key1", "value1")
	if err != nil {
		t.Fatalf("Error adding key %v", err)
	}

	err = s.Delete("key1")
	if err != nil {
		t.Fatalf("Error deleting key: %v", err)
	}
}



