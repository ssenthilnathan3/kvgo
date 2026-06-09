package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/ssenthilnathan3/kvgo/constants"
	"github.com/ssenthilnathan3/kvgo/internal/persistence"
	"github.com/ssenthilnathan3/kvgo/internal/types"
)


func WithStore(ctx context.Context, store *types.Store) context.Context {
	return context.WithValue(ctx, constants.StoreKey, store)
}

func GetStore(ctx context.Context) (*types.Store, bool) {
	user, ok := ctx.Value(constants.StoreKey).(*types.Store)
	return user, ok
}

func Get(ctx context.Context, key string) (string, error) {
	s, exists := GetStore(ctx)
	if !exists {
		fmt.Println("Store not found in context")
		return "", nil
	}

	var value string

	s.Mu.Lock()
	value = s.Data[key]
	s.Mu.Unlock()

	return value, nil
}

func Put(ctx context.Context, key string, value string) error {
	s, exists := GetStore(ctx)
	if !exists {
		fmt.Println("Store not found in context!")
		return nil
	}

	s.Mu.Lock()
	s.Data[key] = value
	s.Mu.Unlock()

	err := persistence.WritePersist(key, value)
	if err != nil {
		fmt.Println("Key stored successfully")
		return err
	}
	return nil
}

func Delete(ctx context.Context, key string) error {
	s, exists := GetStore(ctx)
	if !exists {
		fmt.Println("Store not found in context")
		return nil
	}

	s.Mu.Lock()
	delete(s.Data, key)
	s.Mu.Unlock()

	return nil
}

func LoadPersist(ctx context.Context) (context.Context, error) {
	fileBytes, err := os.ReadFile(constants.DB)
	if err != nil {
		fmt.Printf("Error reading file")
		return nil, err
	}

	var decodedStore types.Store

	err = json.Unmarshal(fileBytes, &decodedStore)
	if err != nil {
		fmt.Printf("Error decoding file")
		return nil, err
	}

	return WithStore(ctx, &decodedStore), nil
}

