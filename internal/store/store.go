package store

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ssenthilnathan3/kvgo/internal/persistence"
)

type Store struct {
	mu sync.RWMutex
	Data map[string]persistence.Entry
	Persister persistence.Persister
	WAL *persistence.WALLoader
}

func (s *Store) Get(key string) (string, error) {
	var entry persistence.Entry

	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.Data[key]
	if !exists {
		err := fmt.Errorf("Value not found")
		return "", err
	}

	return entry.Value, nil
}

func (s *Store) Put(key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Data[key] = persistence.Entry{Value: value, Timestamp: time.Now().UnixNano()}

	var now = time.Now().Unix()
	timestamp := strconv.FormatInt(now, 10)

	return s.WAL.AppendLog(timestamp, "PUT", key, value)
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.Data[key]
	if !exists {
		return fmt.Errorf("Key not found")
	}

	var now = time.Now().Unix()
	timestamp := strconv.FormatInt(now, 10)

	delete(s.Data, key)
	return s.WAL.AppendLog(timestamp, "DELETE", key, "")
}

func (s *Store) Exec(commands []persistence.WAL) error {
	for _, c := range commands {
		switch c.Command {
		case "PUT":
			s.mu.Lock()
			s.Data[c.Key] = persistence.Entry{Value: c.Value.Value, Timestamp: c.Value.Timestamp}
			s.mu.Unlock()
		case "DELETE":
			s.mu.Lock()
			delete(s.Data, c.Key)
			s.mu.Unlock()
		default:
			return fmt.Errorf("No valid command found")
		}
	}
	return nil
}

func (s *Store) TakeSnap() error {
	return s.Persister.Save(s.Data)
}
