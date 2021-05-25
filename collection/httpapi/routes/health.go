package routes

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func AddHealthRoutes(rg *gin.RouterGroup) {
	flows := rg.Group("/health")

	flows.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"healthy": true})
	})
}
