package cluster

import (
	"context"
	"time"
)

func (c *Cluster) ConnectAll() {
	c.Clients = make(map[string]*PeerClient)

	for i := range c.Peers {
		client, err := ConnectToPeer(&c.Peers[i])
		if err != nil {
			continue
		}
		c.Clients[c.Peers[i].ID] = client
	}
}

func (c *Cluster) Replicate(key, value string) error {
	for i := range c.Peers {
		p := &c.Peers[i]
		if !p.Alive.Load() {
			continue
		}

		client, exists := c.Clients[p.ID]
		if !exists {
			continue
		}

		ctx, cancel := context.WithTimeout(
			context.Background(),
			2*time.Second,
		)

		_ = client.Broadcast(ctx, key, value)

		cancel()
	}

	return nil
}
