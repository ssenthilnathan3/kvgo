package cluster

import (
	"context"

	"github.com/ssenthilnathan3/kvgo/internal/store"
	pb "github.com/ssenthilnathan3/kvgo/proto"
)

type CommsServer struct {
	pb.UnimplementedCommsServiceServer
	Store *store.Store
	Config Cluster
}

func (s *CommsServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{Message: "pong"}, nil
}

func (s *CommsServer) Info(ctx context.Context, req *pb.InfoRequest) (*pb.InfoResponse, error) {
	return &pb.InfoResponse{NodeId: s.Config.Self.ID}, nil
}

func (s *CommsServer) Broadcast(ctx context.Context, req *pb.BroadcastRequest) (*pb.BroadcastResponse, error) {
	return &pb.BroadcastResponse{Message: ""}, nil
}
