package cluster

type Node struct {
	ID string
	Host string
	Port int
	Grpc int
	Alive bool
}

type Cluster struct {
	Self Node
	Peers []Node
	Clients map[string]*PeerClient
}
