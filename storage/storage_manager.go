package storage

import (
	"fmt"

	"github.com/sheacloud/flowstore/enrichment"
)

type StorageBackend interface {
	Store(flow *enrichment.EnrichedFlow)
}

type StorageManager struct {
	InputChannel    chan *enrichment.EnrichedFlow
	StopChannel     chan bool
	StorageBackends []StorageBackend
}

func NewStorageManager(input chan *enrichment.EnrichedFlow, backends []StorageBackend) StorageManager {
	return StorageManager{
		InputChannel:    input,
		StopChannel:     make(chan bool),
		StorageBackends: backends,
	}
}

func (sm *StorageManager) Start() {
	go func() {
	InfiniteLoop:
		for {
			select {
			case <-sm.StopChannel:
				break InfiniteLoop
			case flow := <-sm.InputChannel:
				for _, backend := range sm.StorageBackends {
					backend.Store(flow)
				}
			}
		}
		fmt.Println("Enrichment Manager stopped")
	}()
}

func (sm *StorageManager) Stop() {
	sm.StopChannel <- true
}
