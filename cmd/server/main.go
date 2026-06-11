package main

import (
	"log"

	"github.com/gin-gonic/gin"
	"github.com/ssenthilnathan3/kvgo/constants"
	"github.com/ssenthilnathan3/kvgo/internal/api"
	"github.com/ssenthilnathan3/kvgo/internal/persistence"
	"github.com/ssenthilnathan3/kvgo/internal/store"
)

func main() {
	walMaxChan := make(chan struct{})
	stopChan := make(chan struct{})

	persister := &persistence.JSONFilePersister{
		Path: constants.DB,
	}

	loader := &persistence.WALLoader {
		WALPath: constants.WAL,
		WALIndex: 0,
		WALChan: walMaxChan,
	}

	data, err := persister.Load()
	if err != nil {
		log.Fatal(err)
	}

	wal, err := loader.LoadWAL()
	if err != nil {
		log.Fatal(err)
	}

	s := &store.Store{
		Data: data,
		Persister: persister,
		WAL: loader,
	}

	err = s.Exec(wal)
	if err != nil {
		log.Fatal(err)
	}

	h := api.Handler{
		Store: s,
	}

	if loader.WALIndex >= constants.WALMax {
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

	r.POST("/keys", h.CreateKey)
	r.GET("/keys/:key", h.GetKey)
	r.DELETE("/keys/:key", h.DeleteKey)

	r.Run(":8080")
	defer close(stopChan)
}
