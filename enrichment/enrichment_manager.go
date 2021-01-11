package enrichment

import (
	"fmt"
	"net"

	"github.com/vmware/go-ipfix/pkg/entities"
)

type Enricher interface {
	Enrich(flow *EnrichedFlow)
}

type EnrichedFlow struct {
	Fields map[string]interface{}
}

func (ef *EnrichedFlow) GetSourceIP() net.IP {
	return ef.Fields["sourceIPv4Address"].(net.IP)
}

func (ef *EnrichedFlow) GetDestIP() net.IP {
	return ef.Fields["destinationIPv4Address"].(net.IP)
}

func (ef *EnrichedFlow) GetStartTime() uint64 {
	return ef.Fields["flowStartMilliseconds"].(uint64)
}

type EnrichmentManager struct {
	InputChannel  chan *entities.Message
	OutputChannel chan *EnrichedFlow
	StopChannel   chan bool
	Enrichers     []Enricher
}

func NewEnrichmentManager(input chan *entities.Message, output chan *EnrichedFlow, enrichers []Enricher) EnrichmentManager {
	return EnrichmentManager{
		InputChannel:  input,
		OutputChannel: output,
		StopChannel:   make(chan bool),
		Enrichers:     enrichers,
	}
}

func (em *EnrichmentManager) Start() {
	go func() {
	InfiniteLoop:
		for {
			select {
			case <-em.StopChannel:
				break InfiniteLoop
			case msg := <-em.InputChannel:
				set := msg.GetSet()
				if set.GetSetType() == entities.Template {
					fmt.Println("Skipping template set")
					continue
				} else {
					for _, record := range set.GetRecords() {
						enrichedFlow := EnrichedFlow{
							Fields: make(map[string]interface{}),
						}
						for _, ie := range record.GetOrderedElementList() {
							enrichedFlow.Fields[ie.Element.Name] = ie.Value
						}

						for _, enricher := range em.Enrichers {
							enricher.Enrich(&enrichedFlow)
						}

						em.OutputChannel <- &enrichedFlow
					}
				}
			}
		}
		fmt.Println("Enrichment Manager stopped")
	}()
}

func (em *EnrichmentManager) Stop() {
	em.StopChannel <- true
}
