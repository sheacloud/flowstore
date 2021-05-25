package routes

import (
	"fmt"
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sheacloud/flowstore"
)

type FlowModel struct {
	SourceIP               string `json:"source_ip" binding:"required"`
	DestinationIP          string `json:"destination_ip" binding:"required"`
	SourcePort             uint16 `json:"source_port" binding:"required"`
	DestinationPort        uint16 `json:"destination_port" binding:"required"`
	Protocol               uint8  `json:"protocol" binding:"required"`
	FlowStartMilliseconds  uint64 `json:"flow_start_milliseconds" binding:"required"`
	FlowEndMilliseconds    uint64 `json:"flow_end_milliseconds" binding:"required"`
	FlowOctetCount         uint64 `json:"flow_octet_count" binding:"required"`
	FlowPacketCount        uint64 `json:"flow_packet_count" binding:"required"`
	ReverseFlowOctetCount  uint64 `json:"reverse_flow_octet_count"`
	ReverseFlowPacketCount uint64 `json:"reverse_flow_packet_count"`
}

func (f *FlowModel) ToFlow() (*flowstore.Flow, error) {
	flow := &flowstore.Flow{Metadata: make(map[string]interface{})}

	sourceIP := net.ParseIP(f.SourceIP)
	if sourceIP == nil {
		return nil, fmt.Errorf("SourceIP is an invalid IP address: %s", f.SourceIP)
	}
	flow.SourceIP = sourceIP

	destinationIP := net.ParseIP(f.DestinationIP)
	if destinationIP == nil {
		return nil, fmt.Errorf("DestinationIP is an invalid IP address: %s", f.DestinationIP)
	}
	flow.DestinationIP = destinationIP

	flow.SourcePort = f.SourcePort
	flow.DestinationPort = f.DestinationPort
	flow.Protocol = f.Protocol
	flow.FlowStartMilliseconds = f.FlowStartMilliseconds
	flow.FlowEndMilliseconds = f.FlowEndMilliseconds
	flow.FlowOctetCount = f.FlowOctetCount
	flow.FlowPacketCount = f.FlowPacketCount
	flow.ReverseFlowOctetCount = f.ReverseFlowOctetCount
	flow.ReverseFlowPacketCount = f.ReverseFlowPacketCount

	return flow, nil
}

type PostFlowsInput struct {
	Flows []FlowModel `json:"flows" binding:"required,dive"`
}

type PostFlowsOutput struct {
	Error          string `json:"error,omitempty"`
	ProcessedFlows int    `json:"processed_flows,omitempty"`
}

func PostFlows(outputChannel chan *flowstore.Flow) gin.HandlerFunc {
	return func(c *gin.Context) {
		var flowsRequest PostFlowsInput
		err := c.ShouldBind(&flowsRequest)
		if err != nil {
			c.JSON(http.StatusBadRequest, PostFlowsOutput{Error: err.Error()})
			return
		}

		numParsedFlows := 0

		for _, flow := range flowsRequest.Flows {
			parsedFlow, err := flow.ToFlow()
			if err != nil {
				c.JSON(http.StatusBadRequest, PostFlowsOutput{Error: err.Error()})
				return
			}

			outputChannel <- parsedFlow

			numParsedFlows += 1
		}
		c.JSON(http.StatusOK, PostFlowsOutput{ProcessedFlows: numParsedFlows})
	}
}

func AddFlowsRoutes(rg *gin.RouterGroup, outputChannel chan *flowstore.Flow) {
	flows := rg.Group("/flows")

	flows.POST("/", PostFlows(outputChannel))
}
