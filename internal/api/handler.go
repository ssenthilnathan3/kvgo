package api

import (
	"github.com/gin-gonic/gin"
	"github.com/ssenthilnathan3/kvgo/internal/store"
)

type Handler struct {
	Store *store.Store
	Replicate func(key, value string)
}

func (h *Handler) CreateKey(ginCtx *gin.Context) {
	var newKeyValue map[string]string

	if err := ginCtx.ShouldBindJSON(&newKeyValue); err != nil {
		ginCtx.JSON(400,gin.H{"error": err.Error()})
		return
	}

	for key, value := range newKeyValue {
		if err := h.Store.Put(key, value); err != nil {
			ginCtx.JSON(500, gin.H{"error": err.Error()})
			return
		}

		if h.Replicate != nil {
			h.Replicate(key, value)
		}
	}

	ginCtx.JSON(200, gin.H{"message": "success"})
}

func (h *Handler) GetKey(ginCtx *gin.Context) {
	key := ginCtx.Param("key")

	value, err := h.Store.Get(key)
	if err != nil {
		ginCtx.JSON(404, gin.H{"error": "Key not found in database"})
		return
	}

	ginCtx.JSON(200, gin.H{"message": value})
}

func (h *Handler) DeleteKey(ginCtx *gin.Context) {
	key := ginCtx.Param("key")

	err := h.Store.Delete(key)
	if err != nil {
		ginCtx.JSON(500, gin.H{"error": "Error deleting key"})
		return
	}

	ginCtx.JSON(200, gin.H{"message": "success"})
}
