package enrichment

import (
	"github.com/sheacloud/flowstore"
	"github.com/sirupsen/logrus"
)

type Enricher interface {
	Enrich(flow *flowstore.Flow)
	GetName() string
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
		enricherNames := []string{}
		for _, enricher := range em.Enrichers {
			enricherNames = append(enricherNames, enricher.GetName())
		}
		logrus.WithFields(logrus.Fields{
			"enrichers": enricherNames,
		}).Info("Enrichment Manager Started...")
	InfiniteLoop:
		for {
			select {
			case <-em.StopChannel:
				break InfiniteLoop
			case msg := <-em.InputChannel:
				for _, enricher := range em.Enrichers {
					logrus.WithFields(logrus.Fields{
						"flow":     msg.String(),
						"enricher": enricher.GetName(),
					}).Trace("Enriching flow")
					enricher.Enrich(msg)
				}

				em.OutputChannel <- msg
			}
		}
		logrus.Info("Enrichment Manager Stopped")
		em.Stopped <- true
	}()
}

func (em *EnrichmentManager) Stop() {
	em.StopChannel <- true
	<-em.Stopped
}
