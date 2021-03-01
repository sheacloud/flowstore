package collection

import (
	"fmt"
	"net"

	"github.com/sheacloud/flowstore"
	"github.com/vmware/go-ipfix/pkg/collector"
	"github.com/vmware/go-ipfix/pkg/entities"
	"k8s.io/klog"
)

type IpfixCollector struct {
	Address       string
	Port          uint16
	Protocol      string
	OutputChannel chan *flowstore.Flow
	StopChannel   chan bool
	Stopped       chan bool
}

func NewIpfixCollector(address string, port uint16, protocol string, outputChannel chan *flowstore.Flow) *IpfixCollector {
	return &IpfixCollector{
		Address:       address,
		Port:          port,
		Protocol:      protocol,
		OutputChannel: outputChannel,
		StopChannel:   make(chan bool),
		Stopped:       make(chan bool),
	}
}

func convertIpfixToFlow(msg *entities.Message) (*flowstore.Flow, error) {
	set := msg.GetSet()
	if set.GetSetType() == entities.Template {
		return nil, nil
	} else {
		flow := &flowstore.Flow{Metadata: make(map[string]interface{})}

		var ok bool
		for _, record := range set.GetRecords() {
			for _, ie := range record.GetOrderedElementList() {
				switch ie.Element.Name {
				case "sourceIPv4Address":
					flow.SourceIP, ok = ie.Value.(net.IP)
					if !ok {
						klog.Warningf("Couldn't cast %v/%T sourceIPv4Address to net.IP", ie.Value, ie.Value)
						return nil, fmt.Errorf("Couldn't cast sourceIPv4Address to net.IP")
					}
				case "destinationIPv4Address":
					flow.DestinationIP, ok = ie.Value.(net.IP)
					if !ok {
						klog.Warningf("Couldn't cast %v/%T destinationIPv4Address to net.IP", ie.Value, ie.Value)
						return nil, fmt.Errorf("Couldn't cast destinationIPv4Address to net.IP")
					}
				case "sourceTransportPort":
					flow.SourcePort, ok = ie.Value.(uint16)
					if !ok {
						klog.Warningf("Couldn't cast %v/%T sourceTransportPort to uint16", ie.Value, ie.Value)
						return nil, fmt.Errorf("Couldn't cast sourceTransportPort to uint16")
					}
				case "destinationTransportPort":
					flow.DestinationPort, ok = ie.Value.(uint16)
					if !ok {
						klog.Warningf("Couldn't cast %v/%T destinationTransportPort to uint16", ie.Value, ie.Value)
						return nil, fmt.Errorf("Couldn't cast destinationTransportPort to uint16")
					}
				case "protocolIdentifier":
					flow.Protocol, ok = ie.Value.(uint8)
					if !ok {
						klog.Warningf("Couldn't cast %v/%T protocolIdentifier to uint8", ie.Value, ie.Value)
						return nil, fmt.Errorf("Couldn't cast protocolIdentifier to uint8")
					}
				case "flowStartMilliseconds":
					flow.FlowStartMilliseconds, ok = ie.Value.(uint64)
					if !ok {
						klog.Warningf("Couldn't cast %v/%T flowStartMilliseconds to uint64", ie.Value, ie.Value)
						return nil, fmt.Errorf("Couldn't cast flowStartMilliseconds to uint64")
					}
				case "flowEndMilliseconds":
					flow.FlowEndMilliseconds, ok = ie.Value.(uint64)
					if !ok {
						klog.Warningf("Couldn't cast %v/%T flowEndMilliseconds to uint64", ie.Value, ie.Value)
						return nil, fmt.Errorf("Couldn't cast flowEndMilliseconds to uint64")
					}
				case "octetDeltaCount":
					flow.FlowOctetCount, ok = ie.Value.(uint64)
					if !ok {
						klog.Warningf("Couldn't cast %v/%T octetDeltaCount to uint64", ie.Value, ie.Value)
						return nil, fmt.Errorf("Couldn't cast octetDeltaCount to uint64")
					}
				case "packetDeltaCount":
					flow.FlowPacketCount, ok = ie.Value.(uint64)
					if !ok {
						klog.Warningf("Couldn't cast %v/%T packetDeltaCount to uint64", ie.Value, ie.Value)
						return nil, fmt.Errorf("Couldn't cast packetDeltaCount to uint64")
					}
				case "initiatorOctets":
					flow.FlowOctetCount, ok = ie.Value.(uint64)
					if !ok {
						klog.Warningf("Couldn't cast %v/%T initiatorOctets to uint64", ie.Value, ie.Value)
						return nil, fmt.Errorf("Couldn't cast initiatorOctets to uint64")
					}
				case "initiatorPackets":
					flow.FlowPacketCount, ok = ie.Value.(uint64)
					if !ok {
						klog.Warningf("Couldn't cast %v/%T initiatorPackets to uint64", ie.Value, ie.Value)
						return nil, fmt.Errorf("Couldn't cast initiatorPackets to uint64")
					}
				case "responderOctets":
					flow.ReverseFlowOctetCount, ok = ie.Value.(uint64)
					if !ok {
						klog.Warningf("Couldn't cast %v/%T responderOctets to uint64", ie.Value, ie.Value)
						return nil, fmt.Errorf("Couldn't cast responderOctets to uint64")
					}
				case "responderPackets":
					flow.ReverseFlowPacketCount, ok = ie.Value.(uint64)
					if !ok {
						klog.Warningf("Couldn't cast %v/%T responderPackets to uint64", ie.Value, ie.Value)
						return nil, fmt.Errorf("Couldn't cast responderPackets to uint64")
					}
				default:
					klog.Warningf("Rec'd unsupported field %s with value %v", ie.Element.Name, ie.Value)
					// add field to metadata
					flow.Metadata[ie.Element.Name] = ie.Value
				}
			}
		}
		return flow, nil
	}
}

func (ic *IpfixCollector) Start() error {
	cpInput := collector.CollectorInput{
		Address:       fmt.Sprintf("%v:%v", ic.Address, ic.Port),
		Protocol:      ic.Protocol,
		MaxBufferSize: 65535,
		TemplateTTL:   0,
		IsEncrypted:   false,
		ServerCert:    nil,
		ServerKey:     nil,
	}
	cp, err := collector.InitCollectingProcess(cpInput)
	if err != nil {
		return err
	}

	go func() {
		go cp.Start()
		msgChan := cp.GetMsgChan()
	InfiniteLoop:
		for {
			select {
			case <-ic.StopChannel:
				break InfiniteLoop
			case msg := <-msgChan:
				klog.Info("Processing IPFIX message")
				// convert msg to Flow
				flow, err := convertIpfixToFlow(msg)
				if err != nil {
					continue
				}
				if flow == nil {
					continue
				}
				ic.OutputChannel <- flow
			}
		}
		klog.Info("IpfixCollector stopped")
		ic.Stopped <- true
	}()

	return nil
}

func (ic *IpfixCollector) Stop() {
	ic.StopChannel <- true
	<-ic.Stopped
}
