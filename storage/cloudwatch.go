package storage

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/google/uuid"
	"github.com/sheacloud/flowstore"
)

type CloudwatchState struct {
	LogGroupName      string
	LogStream         *LogStream
	CloudwatchLogsSvc *cloudwatchlogs.CloudWatchLogs
}

func (c *CloudwatchState) Initialize() {
	logStreamName := uuid.New().String()

	c.LogStream = &LogStream{
		LogStreamName:       logStreamName,
		LogGroupName:        c.LogGroupName,
		bufferedEvents:      []*cloudwatchlogs.InputLogEvent{},
		bufferedEventsBytes: 0,
		cloudwatchState:     c,
	}

	_, err := c.CloudwatchLogsSvc.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(c.LogGroupName),
		LogStreamName: aws.String(logStreamName),
	})
	if err != nil {
		fmt.Println("error creating log stream")
		return
	}
}

type LogStream struct {
	LogStreamName       string
	LogGroupName        string
	LastSequenceToken   *string
	bufferedEvents      []*cloudwatchlogs.InputLogEvent
	bufferedEventsBytes int
	bufferedEventLock   sync.Mutex
	lastUploadTime      time.Time
	uploadLock          sync.Mutex
	cloudwatchState     *CloudwatchState
}

func (l *LogStream) IngestEvent(flow *flowstore.Flow) {
	data, _ := json.MarshalIndent(flow, "", "  ")
	l.addEventToBuffer(data, int64(flow.FlowStartMilliseconds))
}

func (l *LogStream) addEventToBuffer(message []byte, timestamp int64) {
	event := &cloudwatchlogs.InputLogEvent{
		Message:   aws.String(string(message)),
		Timestamp: aws.Int64(timestamp),
	}
	l.bufferedEventLock.Lock()

	// insert the event into the buffer in a sorted order
	index := sort.Search(len(l.bufferedEvents), func(i int) bool { return *l.bufferedEvents[i].Timestamp >= timestamp }) // get the index where the event should be placed
	l.bufferedEvents = append(l.bufferedEvents, nil)                                                                     // extend the buffer by 1
	copy(l.bufferedEvents[index+1:], l.bufferedEvents[index:])                                                           //shift the data in the buffer back 1
	l.bufferedEvents[index] = event                                                                                      // insert the event into the list

	l.bufferedEventsBytes += len(message) + 26

	var uploadEvents bool

	// TODO figure out better buffer size cutoff to correspond to AWS limit of 1,048,576 bytes
	if l.bufferedEventsBytes >= 500000 {
		uploadEvents = true
		fmt.Println("Uploading events as 100KB buffer has been reached")
	} else if time.Now().Sub(l.lastUploadTime).Seconds() >= 10 {
		uploadEvents = true
		fmt.Println("Uploading events as time limit has been reached")
	} else if len(l.bufferedEvents) > 9000 {
		uploadEvents = true
		fmt.Println("Uploading events as 9k event limit has been reached")
	}
	l.bufferedEventLock.Unlock()

	if uploadEvents {
		l.UploadBufferedEvents()
	}
}

func (l *LogStream) UploadBufferedEvents() {
	// acquire the buffered event lock, copy the data out of the list, and reset it
	l.bufferedEventLock.Lock()
	eventList := make([]*cloudwatchlogs.InputLogEvent, len(l.bufferedEvents))
	copy(eventList, l.bufferedEvents)
	l.bufferedEvents = nil
	l.bufferedEventsBytes = 0
	l.bufferedEventLock.Unlock()

	l.uploadLock.Lock()
	resp, err := l.cloudwatchState.CloudwatchLogsSvc.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
		LogEvents:     eventList,
		LogGroupName:  aws.String(l.LogGroupName),
		LogStreamName: aws.String(l.LogStreamName),
		SequenceToken: l.LastSequenceToken,
	})
	if err != nil {
		fmt.Println("error publishing log events")
		fmt.Println(err.Error())
		l.uploadLock.Unlock()
		return
	}

	l.LastSequenceToken = resp.NextSequenceToken
	fmt.Println(resp.RejectedLogEventsInfo)
	l.lastUploadTime = time.Now()
	l.uploadLock.Unlock()
}

func (c *CloudwatchState) Store(flow *flowstore.Flow) {
	c.LogStream.IngestEvent(flow)
}
