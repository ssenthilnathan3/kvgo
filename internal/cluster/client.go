package cluster

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/ssenthilnathan3/kvgo/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PeerClient struct {
	conn   *grpc.ClientConn
	Client pb.CommsServiceClient
	Mu sync.Mutex
}

func ConnectToPeer(node Node) (*PeerClient, error) {
	addr := fmt.Sprintf("%s:%d", node.Host, node.Grpc)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &PeerClient{
		conn:   conn,
		Client: pb.NewCommsServiceClient(conn),
	}, nil
}

func (p *PeerClient) Ping(ctx context.Context) error {
	_, err := p.Client.Ping(ctx, &pb.PingRequest{})
	return err
}

func (p *PeerClient) Broadcast(ctx context.Context, key, value string) error {
	_, err := p.Client.Broadcast(ctx, &pb.BroadcastRequest{Message: key + "=" + value})
	return err
}

func (p *PeerClient) Close() {
	p.conn.Close()
}
