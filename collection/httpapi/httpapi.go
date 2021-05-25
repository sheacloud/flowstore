package httpapi

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sheacloud/flowstore"
	"github.com/sheacloud/flowstore/collection/httpapi/routes"
	"github.com/sirupsen/logrus"
)

var (
	router = gin.New()
)

func installLogrusLogger(r *gin.Engine) {
	r.Use(func(c *gin.Context) {
		// other handler can change c.Path so:
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery
		start := time.Now()

		// process the request
		c.Next()

		timestamp := time.Now()
		latency := timestamp.Sub(start)

		clientIP := c.ClientIP()
		method := c.Request.Method
		statusCode := c.Writer.Status()
		errorMessage := c.Errors.ByType(gin.ErrorTypePrivate).String()

		bodySize := c.Writer.Size()

		if raw != "" {
			path = path + "?" + raw
		}

		fields := logrus.Fields{
			"status_code": statusCode,
			"latency":     latency, // time to process
			"client_ip":   clientIP,
			"method":      method,
			"path":        path,
			"body_size":   bodySize,
		}

		if errorMessage != "" {
			fields["error"] = errorMessage
		}

		entry := logrus.WithFields(fields)

		msg := "HTTP Request Received"
		if statusCode >= http.StatusInternalServerError {
			entry.Error(msg)
		} else if statusCode >= http.StatusBadRequest {
			entry.Warn(msg)
		} else {
			entry.Info(msg)
		}

	})
}

func installCustomRecovery(r *gin.Engine) {
	r.Use(gin.CustomRecovery(func(c *gin.Context, recovered interface{}) {
		if err, ok := recovered.(string); ok {
			c.String(http.StatusInternalServerError, fmt.Sprintf("error: %s", err))
		}
		c.AbortWithStatus(http.StatusInternalServerError)
	}))
}

func GetRouter(outputChannel chan *flowstore.Flow) (*gin.Engine, error) {
	installLogrusLogger(router)
	installCustomRecovery(router)

	flowstore := router.Group("/flowstore")
	routes.AddFlowsRoutes(flowstore, outputChannel)
	routes.AddHealthRoutes(flowstore)

	return router, nil
}
