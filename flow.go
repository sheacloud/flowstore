package flowstore

import "net"

// Flow represents a netflow with associated metadata
type Flow struct {
	SourceIP               net.IP
	DestinationIP          net.IP
	SourcePort             uint16
	DestinationPort        uint16
	Protocol               uint8
	FlowStartMilliseconds  uint64
	FlowEndMilliseconds    uint64
	FlowOctetCount         uint64
	FlowPacketCount        uint64
	ReverseFlowOctetCount  uint64
	ReverseFlowPacketCount uint64
	Metadata               map[string]interface{}
}
