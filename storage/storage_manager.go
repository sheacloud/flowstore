package storage

import (
	"fmt"

	"github.com/sheacloud/flowstore"
)

type StorageBackend interface {
	Store(flow *flowstore.Flow)
}

type StorageManager struct {
	InputChannel    chan *flowstore.Flow
	StopChannel     chan bool
	StorageBackends []StorageBackend
	Stopped         chan bool
}

func NewStorageManager(input chan *flowstore.Flow, backends []StorageBackend) StorageManager {
	return StorageManager{
		InputChannel:    input,
		StopChannel:     make(chan bool),
		StorageBackends: backends,
		Stopped:         make(chan bool),
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
		fmt.Println("Storage Manager stopped")
		sm.Stopped <- true
	}()
}

func (sm *StorageManager) Stop() {
	sm.StopChannel <- true
	<-sm.Stopped
}
