package store

import (
	"context"
	"errors"
	"sync"
)

var ErrNotFound = errors.New("key not found")

// Store is a concurrency-safe in-memory key-value store.
//
// The map remains encapsulated so future phases can add shard ownership checks
// and routing gates in these methods without changing callers.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func New() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Put(ctx context.Context, key, value string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()
	return nil
}

func (s *Store) Get(ctx context.Context, key string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	s.mu.RLock()
	value, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return "", ErrNotFound
	}

	return value, nil
}

func (s *Store) Count() int {
	s.mu.RLock()
	count := len(s.data)
	s.mu.RUnlock()
	return count
}

func (s *Store) Delete(ctx context.Context, key string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		return ErrNotFound
	}

	delete(s.data, key)
	return nil
}

func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	copyData := make(map[string]string, len(s.data))
	for key, value := range s.data {
		copyData[key] = value
	}

	return copyData
}
