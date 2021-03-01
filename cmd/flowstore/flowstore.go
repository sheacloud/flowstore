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
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"k8s.io/klog/v2"

	"github.com/vmware/go-ipfix/pkg/registry"

	"github.com/sheacloud/flowstore"
	"github.com/sheacloud/flowstore/collection"
	"github.com/sheacloud/flowstore/enrichment"
	"github.com/sheacloud/flowstore/storage"

	goflag "flag"

	_ "net/http/pprof"
)

const (
	logToStdErrFlag = "logtostderr"
)

var (
	IPFIXAddr      string
	IPFIXPort      uint16
	IPFIXTransport string
	ipfixCollector *collection.IpfixCollector

	rootCmd = &cobra.Command{
		Use:  "flowstore",
		Long: "Flow storage utility",
		Run: func(cmd *cobra.Command, args []string) {
			run()
		},
	}
)

func init() {
	addIPFIXFlags(rootCmd.Flags())
	klog.InitFlags(nil)
	goflag.Parse()
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
}

func httpServer() {
	fmt.Println("starting prom server")
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":9090", nil))
}

func addIPFIXFlags(fs *flag.FlagSet) {
	fs.StringVar(&IPFIXAddr, "ipfix.addr", "0.0.0.0", "IPFIX collector address")
	fs.Uint16Var(&IPFIXPort, "ipfix.port", 4739, "IPFIX collector port")
	fs.StringVar(&IPFIXTransport, "ipfix.transport", "tcp", "IPFIX collector transport layer")
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
	klog.Info("Starting IPFIX collector")

	// Load the IPFIX global registry
	registry.LoadRegistry()

	ipfixOutput := make(chan *flowstore.Flow)
	ipfixCollector = collection.NewIpfixCollector(IPFIXAddr, IPFIXPort, IPFIXTransport, ipfixOutput)
	ipfixCollector.Start()

	geo := enrichment.GeoIPEnricher{
		Language: "en",
	}
	geo.Initialize()

	enrichmentOutput := make(chan *flowstore.Flow)
	enrichmentManager := enrichment.NewEnrichmentManager(ipfixOutput, enrichmentOutput, []enrichment.Enricher{&geo})
	enrichmentManager.Start()

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	cloudwatchLogsSvc := cloudwatchlogs.New(sess)

	cloudwatch := storage.CloudwatchState{
		LogGroupName:      "/goflow/",
		CloudwatchLogsSvc: cloudwatchLogsSvc,
	}

	cloudwatch.Initialize()
	//
	// timestreamSvc := timestreamwrite.New(sess)
	// timestream := storage.TimestreamState{
	// 	DatabaseName:  "flowstore",
	// 	TableName:     "flows",
	// 	TimestreamSvc: timestreamSvc,
	// }

	klogStorage := storage.KlogState{}

	// elasticsearchState := storage.NewElasticsearchState("bleh", "flowstore")

	storageManager := storage.NewStorageManager(enrichmentOutput, []storage.StorageBackend{&cloudwatch, &klogStorage})
	storageManager.Start()

	stopCh := make(chan struct{})
	go signalHandler(stopCh)

	<-stopCh
	klog.Info("Stopping flowstore")
	ipfixCollector.Stop()
	enrichmentManager.Stop()
	storageManager.Stop()
	return nil
}

func main() {
	defer klog.Flush()

	go httpServer()

	if err := rootCmd.Execute(); err != nil {
		klog.Flush()
		os.Exit(1)
	}
}
