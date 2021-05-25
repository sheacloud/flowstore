package storage

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/sheacloud/flowstore"
	"github.com/sirupsen/logrus"
)

var csvHeader = []string{
	"SourceIP",
	"DestinationIP",
	"SourcePort",
	"DestinationPort",
	"Protocol",
	"FlowStartMilliseconds",
	"FlowEndMilliseconds",
	"FlowOctetCount",
	"FlowPacketCount",
	"ReverseFlowOctetCount",
	"ReverseFlowPacketCount",
}

type S3State struct {
	S3Client      *s3.Client
	ActiveObject  *s3Object
	BucketName    string
	MaxObjectSize int
}

func (s *S3State) Initialize() {
	s.ActiveObject = newS3Object(s.BucketName, s)
	return
}

type s3Object struct {
	objectName           string
	objectPath           string
	bucketName           string
	bufferedEvents       *bytes.Buffer
	bufferedEventsWriter *csv.Writer
	bufferedEventLock    sync.Mutex
	lastUploadTime       time.Time
	uploadLock           sync.Mutex
	s3State              *S3State
}

func newS3Object(bucketName string, s3State *S3State) *s3Object {
	buffer := bytes.NewBuffer([]byte{})

	currentTime := time.Now()
	path := fmt.Sprintf("%v/%v/%v/%v", currentTime.Year(), currentTime.Month(), currentTime.Day(), currentTime.Hour())

	object := &s3Object{
		objectName:           fmt.Sprintf("%s.csv", uuid.New().String()),
		objectPath:           path,
		bucketName:           bucketName,
		bufferedEvents:       buffer,
		bufferedEventsWriter: csv.NewWriter(buffer),
		lastUploadTime:       time.Now(),
		s3State:              s3State,
	}

	object.bufferedEventsWriter.Write(csvHeader)

	return object
}

func (o *s3Object) ingestEvent(flow *flowstore.Flow) {
	row := flattenFlow(flow)
	err := o.bufferedEventsWriter.Write(row)
	if err != nil {
		panic(err)
	}
	o.bufferedEventsWriter.Flush()

	if o.bufferedEvents.Len() >= o.s3State.MaxObjectSize {
		logrus.WithFields(logrus.Fields{
			"s3_bucket_name":      o.bucketName,
			"s3_object_path":      o.objectPath,
			"s3_object_name":      o.objectName,
			"current_object_size": o.bufferedEvents.Len(),
			"max_object_size":     o.s3State.MaxObjectSize,
		}).Info("Uploading events to S3")
		o.uploadBufferedEvents()
	}

	return
}

func (o *s3Object) uploadBufferedEvents() {
	// put obejct in S3
	bytesReader := bytes.NewReader(o.bufferedEvents.Bytes())
	_, err := o.s3State.S3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:          aws.String(o.bucketName),
		Key:             aws.String(fmt.Sprintf("%s/%s", o.objectPath, o.objectName)),
		Body:            bytesReader,
		ContentEncoding: aws.String("text/csv"),
		ContentType:     aws.String("text/csv"),
	})

	if err != nil {
		panic(err)
	}

	o.s3State.ActiveObject = newS3Object(o.bucketName, o.s3State)
	return
}

// Store ingests a flow into the buffer and uploads it to S3 when the size/time threshold is met
func (s *S3State) Store(flow *flowstore.Flow) {
	s.ActiveObject.ingestEvent(flow)
}

func (s *S3State) Flush() {
	s.ActiveObject.uploadBufferedEvents()
}

func (s *S3State) GetName() string {
	return fmt.Sprintf("S3 %s", s.BucketName)
}

func flattenFlow(flow *flowstore.Flow) []string {
	response := make([]string, 11)
	response[0] = flow.SourceIP.String()
	response[1] = flow.DestinationIP.String()
	response[2] = strconv.FormatUint(uint64(flow.SourcePort), 10)
	response[3] = strconv.FormatUint(uint64(flow.DestinationPort), 10)
	response[4] = strconv.FormatUint(uint64(flow.Protocol), 10)
	response[5] = strconv.FormatUint(flow.FlowStartMilliseconds, 10)
	response[6] = strconv.FormatUint(flow.FlowEndMilliseconds, 10)
	response[7] = strconv.FormatUint(flow.FlowOctetCount, 10)
	response[8] = strconv.FormatUint(flow.FlowPacketCount, 10)
	response[9] = strconv.FormatUint(flow.ReverseFlowOctetCount, 10)
	response[10] = strconv.FormatUint(flow.ReverseFlowPacketCount, 10)

	return response
}
