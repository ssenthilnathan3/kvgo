package types

import "sync"

type Data map[string]string

type Store struct {
	Mu sync.RWMutex
	Data Data
}

type CtxKey string
