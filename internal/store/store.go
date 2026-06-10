package store

import (
	"fmt"
	"sync"

	"github.com/ssenthilnathan3/kvgo/internal/persistence"
)


type Store struct {
	Mu sync.RWMutex
	Data map[string]string
	Persister persistence.Persister
}

func (s *Store) Get(key string) (string, error) {
	var value string

	s.Mu.RLock()
	defer s.Mu.RUnlock()

	value, exists := s.Data[key]
	if !exists {
		err := fmt.Errorf("Value not found")
		return "", err
	}

	return value, nil
}

func (s *Store) Put(key string, value string) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	s.Data[key] = value

	return s.Persister.Save(s.Data)
}

func (s *Store) Delete(key string) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	_, exists := s.Data[key]
	if !exists {
		err := fmt.Errorf("Key not found")
		return err
	}

	delete(s.Data, key)

	return s.Persister.Save(s.Data)
}

