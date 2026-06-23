package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ssenthilnathan3/kvgo/internal/api"
	"github.com/ssenthilnathan3/kvgo/internal/cluster"
	"github.com/ssenthilnathan3/kvgo/internal/persistence"
	"github.com/ssenthilnathan3/kvgo/internal/store"
	pb "github.com/ssenthilnathan3/kvgo/proto"
	"google.golang.org/grpc"
)

type Seed struct {
	ID   string `json:"id"`
	Host string `json:"host"`
	Port int    `json:"port"`
	Grpc int    `json:"grpc"`
}

type SeedFile struct {
	Seeds []Seed `json:"seeds"`
}

func NewCluster(nodeID string) (*cluster.Cluster, error) {
	file, err := os.Open("seeds.json")
	if err != nil {
		return nil, fmt.Errorf("Failed to open seeds.json: %v", err)
	}
	defer file.Close()

	var config SeedFile
	if err := json.NewDecoder(file).Decode(&config); err != nil {
		return nil, fmt.Errorf("Failed to parse seeds.json: %v", err)
	}

	var self *Seed
	peers := make([]cluster.Node, 0)

	for _, s := range config.Seeds {
		if s.ID == nodeID {
			self = &s
		} else {
			peers = append(peers, cluster.Node{
				ID:   s.ID,
				Host: s.Host,
				Port: s.Port,
				Grpc: s.Grpc,
			})
		}
	}

	if self == nil {
		return nil, fmt.Errorf("Node ID %q not found in seeds.json", nodeID)
	}

	cluster_cons := &cluster.Cluster{
		Self: cluster.Node{
			ID:    self.ID,
			Host:  self.Host,
			Port:  self.Port,
			Grpc:  self.Grpc,
		},
		Peers: peers,
	}
	cluster_cons.Self.Alive.Store(true)
	return cluster_cons, nil
}

func NewServer(
	clusterConfig *cluster.Cluster,
	stopChan chan struct{},
) (*gin.Engine, *grpc.Server, error) {

	walMaxChan := make(chan struct{})

	persister := &persistence.JSONFilePersister{
		Path: persistence.DB,
	}

	loader := &persistence.WALLoader{
		WALPath:  persistence.WALPath,
		WALIndex: 0,
		WALMax:   persistence.WALMax,
		WALChan:  walMaxChan,
	}

	data, err := persister.Load()
	if err != nil {
		return nil, nil, err
	}

	wal, err := loader.LoadWAL()
	if err != nil {
		return nil, nil, err
	}

	s := &store.Store{
		Data:      data,
		Persister: persister,
		WAL:       loader,
	}

	if err := s.Exec(wal); err != nil {
		return nil, nil, err
	}

	h := api.Handler{
		Store: s,
		Replicate: func(key, value string) {
			clusterConfig.Replicate(key, value)
		},
	}

	if loader.WALIndex >= persistence.WALMax {
		select {
		case walMaxChan <- struct{}{}:
		default:
		}
	}

	go func() {
		for {
			select {
			case <-walMaxChan:
				if err := s.TakeSnap(); err != nil {
					log.Printf("snapshot error: %v", err)
				}

				if err := s.WAL.TruncateLog(); err != nil {
					log.Printf("wal truncation error: %v", err)
				}

			case <-stopChan:
				return
			}
		}
	}()

	r := gin.Default()
	grpcServer := grpc.NewServer()

	r.POST("/keys", h.CreateKey)
	r.GET("/keys/:key", h.GetKey)
	r.DELETE("/keys/:key", h.DeleteKey)

	grpcSrv := &cluster.CommsServer{
		Store:  s,
		Config: clusterConfig,
	}

	pb.RegisterCommsServiceServer(grpcServer, grpcSrv)

	return r, grpcServer, nil
}

func RunServer(clusterConfig *cluster.Cluster) error {
	stopChan := make(chan struct{})

	r, grpcServer, err := NewServer(clusterConfig, stopChan)
	if err != nil {
		return fmt.Errorf("Error creating server: %v", err)
	}

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(clusterConfig.Self.Grpc))
	if err != nil {
		return fmt.Errorf("Error creating listener: %v", err)
	}

	go func() { log.Println(grpcServer.Serve(lis)) }()

	// Heartbeat loop for each peer
	for i := range clusterConfig.Peers {
		peer := &clusterConfig.Peers[i]
		client, err := cluster.ConnectToPeer(peer)
		if err != nil {
			log.Printf("Failed to connect to peer %s: %v", peer.ID, err)
			continue
		}

		go func(c *cluster.PeerClient, p *cluster.Node) {
			ticker := time.NewTicker(5 * time.Second)
			for range ticker.C {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				pingErr := c.Ping(ctx)
				cancel()
				if pingErr != nil {
					p.Alive.Store(false)
					log.Printf("Peer %s unreachable: %v", p.ID, pingErr)
				} else if !p.Alive.Load() {
					p.Alive.Store(true)
				}
			}
		}(client, peer)
	}

	clusterConfig.ConnectAll()

	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(clusterConfig.Self.Port),
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down...")
	close(stopChan)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}

	grpcServer.GracefulStop()
	lis.Close()
	return nil
}
