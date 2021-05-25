// Copyright 2020 VMware, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"k8s.io/klog/v2"

	"github.com/vmware/go-ipfix/pkg/registry"

	"github.com/sheacloud/flowstore"
	"github.com/sheacloud/flowstore/collection/httpapi"
	"github.com/sheacloud/flowstore/collection/ipfix"
	"github.com/sheacloud/flowstore/enrichment"
	"github.com/sheacloud/flowstore/storage"

	_ "net/http/pprof"
)

const (
	logToStdErrFlag = "logtostderr"
)

var (
	cloudwatchViper = viper.New()
	s3Viper         = viper.New()
	ipfixViper      = viper.New()
	apiViper        = viper.New()
	prometheusViper = viper.New()
	logViper        = viper.New()

	logLevel  string
	logCaller bool

	rootCmd = &cobra.Command{
		Use:  "flowstore",
		Long: "Flow storage utility",
		Run: func(cmd *cobra.Command, args []string) {
			run()
		},
	}
)

func initCloudwatchOptions() {

	cloudwatchViper.SetEnvPrefix("cloudwatch")
	cloudwatchViper.AutomaticEnv()

	cloudwatchViper.BindEnv("log_group_name")
	cloudwatchViper.SetDefault("log_group_name", "/goflow/")

	cloudwatchViper.BindEnv("max_buffer_entries")
	cloudwatchViper.SetDefault("max_buffer_entries", 1000)

	cloudwatchViper.BindEnv("max_buffer_size")
	cloudwatchViper.SetDefault("max_buffer_size", 500000)

	cloudwatchViper.BindEnv("buffer_time_limit")
	cloudwatchViper.SetDefault("buffer_time_limit", 10)
}

func initS3Options() {
	s3Viper.SetEnvPrefix("s3")
	s3Viper.AutomaticEnv()

	s3Viper.BindEnv("bucket_name")
	s3Viper.SetDefault("bucket_name", "sheacloud-goflow")

	s3Viper.BindEnv("max_object_size")
	s3Viper.SetDefault("max_object_size", 10000)
}

func initIpfixOptions() {
	ipfixViper.SetEnvPrefix("ipfix")
	ipfixViper.AutomaticEnv()

	ipfixViper.BindEnv("enable")
	ipfixViper.SetDefault("enable", true)

	ipfixViper.BindEnv("addr")
	ipfixViper.SetDefault("addr", "0.0.0.0")

	ipfixViper.BindEnv("port")
	ipfixViper.SetDefault("port", 4739)

	ipfixViper.BindEnv("protocol")
	ipfixViper.SetDefault("protocol", "tcp")
}

func initApiOptions() {
	apiViper.SetEnvPrefix("API")
	apiViper.AutomaticEnv()

	apiViper.BindEnv("enable")
	apiViper.SetDefault("enable", true)

	apiViper.BindEnv("addr")
	apiViper.SetDefault("addr", "0.0.0.0")

	apiViper.BindEnv("port")
	apiViper.SetDefault("port", 8080)
}

func initPrometheusOptions() {
	prometheusViper.SetEnvPrefix("prometheus")
	prometheusViper.AutomaticEnv()

	prometheusViper.BindEnv("addr")
	prometheusViper.SetDefault("addr", "0.0.0.0")

	prometheusViper.BindEnv("port")
	prometheusViper.SetDefault("port", "9090")

	prometheusViper.BindEnv("path")
	prometheusViper.SetDefault("path", "/metrics")
}

func initLogOptions() {
	logViper.SetEnvPrefix("log")
	logViper.AutomaticEnv()

	logViper.BindEnv("level")
	logViper.SetDefault("level", "info")

	logViper.BindEnv("caller")
	logViper.SetDefault("caller", false)
}

func initLogging() {
	// disable klog logging to mute underlying go-ipfix library
	klog.InitFlags(nil)
	flag.Set("logtostderr", "false")
	flag.Set("alsologtostderr", "false")
	klog.SetOutput(ioutil.Discard)

	logrus.SetReportCaller(logViper.GetBool("caller"))
	logrus.SetOutput(os.Stdout)
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	switch strings.ToLower(logViper.GetString("level")) {
	case "panic":
		logrus.SetLevel(logrus.PanicLevel)
	case "fatal":
		logrus.SetLevel(logrus.FatalLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	case "warning":
		logrus.SetLevel(logrus.WarnLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "trace":
		logrus.SetLevel(logrus.TraceLevel)
	default:
		fmt.Printf("Invalid log level %s - valid options are trace, debug, info, warning, error, fatal, panic\n", logLevel)
		os.Exit(1)
	}
}

func init() {
	initCloudwatchOptions()
	initS3Options()
	initIpfixOptions()
	initApiOptions()
	initPrometheusOptions()
	initLogOptions()

	initLogging()
}

func prometheusServer() {
	logrus.WithFields(logrus.Fields{
		"addr": prometheusViper.GetString("addr"),
		"port": prometheusViper.GetString("port"),
		"path": prometheusViper.GetString("path"),
	}).Info("Starting Prometheus...")

	http.Handle(prometheusViper.GetString("path"), promhttp.Handler())
	logrus.Fatal(http.ListenAndServe(fmt.Sprintf(":%v", prometheusViper.GetString("port")), nil))
}

func signalHandler(stopCh chan struct{}) {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		select {
		case <-signalCh:
			close(stopCh)
			return
		}
	}
}

func run() error {
	// Load the IPFIX global registry
	registry.LoadRegistry()

	flowChannel := make(chan *flowstore.Flow)

	enableIpfix := ipfixViper.GetBool("enable")
	enableApi := apiViper.GetBool("enable")

	var ipfixCollector *ipfix.IpfixCollector

	if enableIpfix {
		ipfixCollector = ipfix.NewIpfixCollector(ipfixViper.GetString("addr"), uint16(ipfixViper.GetUint("port")), ipfixViper.GetString("protocol"), flowChannel)
		ipfixCollector.Start()
	}

	if enableApi {
		httpRouter, _ := httpapi.GetRouter(flowChannel)
		go func() {
			httpRouter.Run(fmt.Sprintf("%s:%v", apiViper.GetString("addr"), apiViper.GetUint("port")))
		}()
	}

	geo := enrichment.GeoIPEnricher{
		Language: "en",
	}
	geo.Initialize()

	enrichedFlowChannel := make(chan *flowstore.Flow)
	enrichmentManager := enrichment.NewEnrichmentManager(flowChannel, enrichedFlowChannel, []enrichment.Enricher{&geo})
	enrichmentManager.Start()

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	cloudwatchLogsSvc := cloudwatchlogs.New(sess)

	cloudwatch := storage.CloudwatchState{
		LogGroupName:      cloudwatchViper.GetString("log_group_name"),
		MaxBufferSize:     cloudwatchViper.GetInt("max_buffer_size"),
		MaxBufferEntries:  cloudwatchViper.GetInt("max_buffer_entries"),
		BufferTimeLimit:   cloudwatchViper.GetInt("buffer_time_limit"),
		CloudwatchLogsSvc: cloudwatchLogsSvc,
	}

	cloudwatch.Initialize()

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		panic(err)
	}

	s3Client := s3.NewFromConfig(cfg)

	s3Storage := storage.S3State{
		S3Client:      s3Client,
		BucketName:    s3Viper.GetString("bucket_name"),
		MaxObjectSize: s3Viper.GetInt("max_object_size"),
	}

	s3Storage.Initialize()

	storageManager := storage.NewStorageManager(enrichedFlowChannel, []storage.StorageBackend{&cloudwatch, &s3Storage})
	storageManager.Start()

	stopCh := make(chan struct{})
	go signalHandler(stopCh)

	<-stopCh
	logrus.Info("Stopping flowstore")
	if enableIpfix {
		ipfixCollector.Stop()
	}
	enrichmentManager.Stop()
	storageManager.Stop()
	return nil
}

func main() {
	go prometheusServer()

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
