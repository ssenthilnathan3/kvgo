package main

import (
	"log"
	"os"

	"github.com/ssenthilnathan3/kvgo/internal/app"
)

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		log.Fatal("NODE_ID env var is required (e.g., node1, node2, node3)")
	}

	clusterConfig, err := app.NewCluster(nodeID)
	if err != nil {
		log.Fatalf("Error creating cluster config: %v", err)
	}

	err = app.RunServer(clusterConfig)
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
