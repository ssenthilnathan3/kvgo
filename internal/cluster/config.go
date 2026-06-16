package cluster

type Node struct {
	ID string
	Host string
	Port int
	Grpc int
}

type Cluster struct {
	Self Node
	Peers []Node
}
