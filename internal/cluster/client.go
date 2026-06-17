package cluster

import (
	"context"
	"fmt"

	pb "github.com/ssenthilnathan3/kvgo/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PeerClient struct {
	Node   Node
	conn   *grpc.ClientConn
	Client pb.CommsServiceClient
}

func ConnectToPeer(peer Node) (*PeerClient, error) {
	addr := fmt.Sprintf("%s:%d", peer.Host, peer.Grpc)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &PeerClient{
		Node:   peer,
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
