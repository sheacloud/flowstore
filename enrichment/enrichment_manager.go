package enrichment

import (
	"fmt"

	"github.com/sheacloud/flowstore"
)

type Enricher interface {
	Enrich(flow *flowstore.Flow)
}

type EnrichmentManager struct {
	InputChannel  chan *flowstore.Flow
	OutputChannel chan *flowstore.Flow
	StopChannel   chan bool
	Stopped       chan bool
	Enrichers     []Enricher
}

func NewEnrichmentManager(input chan *flowstore.Flow, output chan *flowstore.Flow, enrichers []Enricher) EnrichmentManager {
	return EnrichmentManager{
		InputChannel:  input,
		OutputChannel: output,
		StopChannel:   make(chan bool),
		Enrichers:     enrichers,
		Stopped:       make(chan bool),
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
				for _, enricher := range em.Enrichers {
					enricher.Enrich(msg)
				}

				em.OutputChannel <- msg
			}
		}
		fmt.Println("Enrichment Manager stopped")
		em.Stopped <- true
	}()
}

func (em *EnrichmentManager) Stop() {
	em.StopChannel <- true
	<-em.Stopped
}
