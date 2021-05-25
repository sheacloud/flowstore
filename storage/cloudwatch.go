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
	"github.com/sirupsen/logrus"
)

type CloudwatchState struct {
	LogGroupName      string
	LogStream         *LogStream
	CloudwatchLogsSvc *cloudwatchlogs.CloudWatchLogs
	MaxBufferSize     int
	BufferTimeLimit   int
	MaxBufferEntries  int
}

func (c *CloudwatchState) Initialize() {
	logStreamName := uuid.New().String()

	c.LogStream = &LogStream{
		LogStreamName:       logStreamName,
		LogGroupName:        c.LogGroupName,
		bufferedEvents:      []*cloudwatchlogs.InputLogEvent{},
		bufferedEventsBytes: 0,
		maxBufferSize:       c.MaxBufferSize,
		maxBufferEntries:    c.MaxBufferEntries,
		bufferTimeLimit:     c.BufferTimeLimit,
		cloudwatchState:     c,
	}

	_, err := c.CloudwatchLogsSvc.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(c.LogGroupName),
		LogStreamName: aws.String(logStreamName),
	})
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"log_group_name":  c.LogGroupName,
			"log_stream_name": logStreamName,
			"error":           err,
		}).Error("Error creating cloudwatch log stream")
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
	maxBufferSize       int
	maxBufferEntries    int
	bufferTimeLimit     int
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
	if l.bufferedEventsBytes >= l.maxBufferSize {
		uploadEvents = true
		logrus.WithFields(logrus.Fields{
			"log_stream_name":     l.LogStreamName,
			"log_group_name":      l.LogGroupName,
			"current_buffer_size": l.bufferedEventsBytes,
			"max_buffer_size":     l.maxBufferSize,
		}).Info("Uploading Cloudwatch Log Events as buffer size limit has been reached")
	} else if time.Now().Sub(l.lastUploadTime).Seconds() >= float64(l.bufferTimeLimit) {
		uploadEvents = true
		logrus.WithFields(logrus.Fields{
			"log_stream_name":           l.LogStreamName,
			"log_group_name":            l.LogGroupName,
			"seconds_since_last_upload": time.Now().Sub(l.lastUploadTime).Seconds(),
			"buffer_time_limit":         l.bufferTimeLimit,
		}).Info("Uploading Cloudwatch Log Events as time limit has been reached")
	} else if len(l.bufferedEvents) > l.maxBufferEntries {
		uploadEvents = true
		logrus.WithFields(logrus.Fields{
			"log_stream_name":        l.LogStreamName,
			"log_group_name":         l.LogGroupName,
			"current_buffer_entries": len(l.bufferedEvents),
			"max_buffer_entries":     l.maxBufferEntries,
		}).Info("Uploading Cloudwatch Log Events as buffer entries limit has been reached")
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
		logrus.WithFields(logrus.Fields{
			"log_group_name":  l.LogGroupName,
			"log_stream_name": l.LogStreamName,
			"error":           err,
		}).Error("Error publishing Cloudwatch Log Events")
		l.uploadLock.Unlock()
		return
	} else if resp.RejectedLogEventsInfo != nil {
		fields := logrus.Fields{}
		if resp.RejectedLogEventsInfo.ExpiredLogEventEndIndex != nil {
			fields["expired_log_event_index"] = *resp.RejectedLogEventsInfo.ExpiredLogEventEndIndex
		}
		if resp.RejectedLogEventsInfo.TooNewLogEventStartIndex != nil {
			fields["too_new_log_event_index"] = *resp.RejectedLogEventsInfo.TooNewLogEventStartIndex
		}
		if resp.RejectedLogEventsInfo.TooOldLogEventEndIndex != nil {
			fields["too_old_log_event_index"] = *resp.RejectedLogEventsInfo.TooOldLogEventEndIndex
		}
		logrus.WithFields(fields).Error("Error publishing Cloudwatch Log Events")
	}

	l.LastSequenceToken = resp.NextSequenceToken
	l.lastUploadTime = time.Now()
	l.uploadLock.Unlock()
}

func (c *CloudwatchState) Store(flow *flowstore.Flow) {
	c.LogStream.IngestEvent(flow)
}

func (c *CloudwatchState) Flush() {
	c.LogStream.UploadBufferedEvents()
}

func (c *CloudwatchState) GetName() string {
	return fmt.Sprintf("Cloudwatch %s", c.LogGroupName)
}
