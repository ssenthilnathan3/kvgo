package api

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/ssenthilnathan3/kvgo/internal/store"
)

func CreateKey(ctx context.Context, ginCtx *gin.Context) {
	var newKeyValue map[string]string

	if err := ginCtx.ShouldBindJSON(&newKeyValue); err != nil {
		ginCtx.JSON(400,gin.H{"error": err.Error()})
		return
	}

	for key, value := range newKeyValue {
		if err := store.Put(ctx, key, value); err != nil {
			ginCtx.JSON(500, gin.H{"error": err.Error()})
			return
		}
		break
	}

	ginCtx.JSON(200, gin.H{"message": "success"})
}

func GetKey(ctx context.Context, ginCtx *gin.Context) {
	var key string

	if err := ginCtx.ShouldBindJSON(&key); err != nil {
		ginCtx.JSON(400, gin.H{"error": err.Error()})
		return
	}

	value, err := store.Get(ctx, key)
	if err != nil {
		ginCtx.JSON(404, gin.H{"error": "Key not found in database"})
		return
	}

	ginCtx.JSON(200, gin.H{"message": value})
}
