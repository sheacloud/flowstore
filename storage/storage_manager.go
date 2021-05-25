package storage

import (
	"github.com/sheacloud/flowstore"
	"github.com/sirupsen/logrus"
)

type StorageBackend interface {
	Store(flow *flowstore.Flow)
	GetName() string
	Flush()
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
		storageBackends := []string{}
		for _, backend := range sm.StorageBackends {
			storageBackends = append(storageBackends, backend.GetName())
		}
		logrus.WithFields(logrus.Fields{
			"storage_backends": storageBackends,
		}).Info("Storage Manager Started...")

	InfiniteLoop:
		for {
			select {
			case <-sm.StopChannel:
				for _, backend := range sm.StorageBackends {
					backend.Flush()
					logrus.WithFields(logrus.Fields{
						"storage_backend": backend.GetName(),
					}).Info("Backend flushed")
				}
				break InfiniteLoop
			case flow := <-sm.InputChannel:
				for _, backend := range sm.StorageBackends {
					logrus.WithFields(logrus.Fields{
						"flow":            flow.String(),
						"storage_backend": backend.GetName(),
					}).Trace("Flow stored")

					backend.Store(flow)
				}
			}
		}
		logrus.Info("Storage Manager Stopped")
		sm.Stopped <- true
	}()
}

func (sm *StorageManager) Stop() {
	sm.StopChannel <- true
	<-sm.Stopped
}
