package cluster

import (
	"sync"
	"sync/atomic"
)

type RoleType int

const (
	Unknown RoleType = iota
	Follower
	Candidate
	Leader
)

type Node struct {
	ID string
	Host string
	Port int
	Grpc int
	Alive atomic.Bool

	Term string
	Role RoleType
}

type Cluster struct {
	mu sync.RWMutex
	Self Node
	Peers []Node
	Clients map[string]*PeerClient
}
