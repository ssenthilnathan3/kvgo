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
	persister := &persistence.JSONFilePersister{
		Path: constants.DB,
	}

	data, err := persister.Load()
	if err != nil {
		log.Fatal(err)
	}

	s := &store.Store{
		Data: data,
		Persister: persister,
	}

	h := api.Handler{
		Store: s,
	}

	r := gin.Default()

	r.POST("/keys", h.CreateKey)
	r.GET("/keys/:key", h.GetKey)
	r.DELETE("/keys/:key", h.DeleteKey)

	r.Run(":8080")
}
