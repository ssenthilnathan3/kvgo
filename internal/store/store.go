package store

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ssenthilnathan3/kvgo/internal/persistence"
)


type Store struct {
	Mu sync.RWMutex
	Data map[string]string
	Persister persistence.Persister
	WAL *persistence.WALLoader
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

	var now = time.Now().Unix()
	timestamp := strconv.FormatInt(now, 10)

	return s.WAL.AppendLog(timestamp, "PUT", key, value)
}

func (s *Store) Delete(key string) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

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
			s.Mu.Lock()
			s.Data[c.Key] = c.Value
			s.Mu.Unlock()
		case "DELETE":
			s.Mu.Lock()
			delete(s.Data, c.Key)
			s.Mu.Unlock()
		default:
			return fmt.Errorf("No valid command found")
		}
	}
	return nil
}

func (s *Store) TakeSnap() error {
	return s.Persister.Save(s.Data)
}
