// Copyright 2019, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package envoyreceiver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/census-instrumentation/opencensus-service/consumer"
	"github.com/census-instrumentation/opencensus-service/observability"

	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	ocmetricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	resourcepb "github.com/census-instrumentation/opencensus-proto/gen-go/resource/v1"
	"github.com/census-instrumentation/opencensus-service/data"
	"github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	metricspb "github.com/envoyproxy/go-control-plane/envoy/service/metrics/v2"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/golang/protobuf/ptypes/wrappers"
	"google.golang.org/api/support/bundler"
	"google.golang.org/grpc"
	prometheus "istio.io/gogo-genproto/prometheus"
)

// Receiver is the type that exposes Trace and Metrics reception.
type Receiver struct {
	mu                  sync.Mutex
	ln                  net.Listener
	serverGRPC          *grpc.Server
	grpcServerOptions   []grpc.ServerOption
	protoMetricsBundler *bundler.Bundler

	metricsConsumer consumer.MetricsConsumer

	stopOnce                 sync.Once
	startServerOnce          sync.Once
	startMetricsReceiverOnce sync.Once
}

type metricsdb struct {
	node *core.Node
	mfes map[string]*mfEntry
}

type mfEntry struct {
	mf          *prometheus.MetricFamily
	metricMap   map[string]*prometheus.Metric
	rename      bool
	name        string
	labelKeys   []*ocmetricspb.LabelKey
	labelValues []*ocmetricspb.LabelValue
}

var (
	errAlreadyStarted          = errors.New("already started")
	errAlreadyStopped          = errors.New("already stopped")
	errHistBucketLenNotEqual   = errors.New("histogram bucket length not equal")
	errHistBucketBoundNotEqual = errors.New("histogram bucket bound not equal")
)

const (
	source      = "EnvoyReceiver"
	defaultAddr = ":55700"
)

// New just creates the Envoy receiver services. It is the caller's
// responsibility to invoke the respective Start*Reception methods as well
// as the various Stop*Reception methods or simply Stop to end it.
func New(addr string, mc consumer.MetricsConsumer) (*Receiver, error) {
	if addr == "" {
		addr = defaultAddr
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("Failed to bind to address %q: %v", addr, err)
	}

	ir := &Receiver{
		ln: ln,
	}

	ir.metricsConsumer = mc
	return ir, nil
}

// MetricsSource returns the name of the metrics data source.
func (ir *Receiver) MetricsSource() string {
	return source
}

// StartMetricsReception exclusively runs the Metrics receiver on the gRPC server.
// To start both Trace and Metrics receivers/services, please use Start.
func (ir *Receiver) StartMetricsReception(ctx context.Context, asyncErrorChan chan<- error) error {
	err := ir.registerMetricsConsumer()
	if err != nil && err != errAlreadyStarted {
		return err
	}
	return ir.startServer()
}

func (ir *Receiver) StreamMetrics(stream metricspb.MetricsService_StreamMetricsServer) error {
	var db *metricsdb
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&metricspb.StreamMetricsResponse{})
		}

		if db == nil {
			id := msg.GetIdentifier()
			if id != nil && id.Node != nil {
				db = &metricsdb{
					node: id.Node,
					mfes: map[string]*mfEntry{},
				}
				log.Printf("initialize node-id %s", db.node.Id)
			}
		}
		if db != nil {
			ir.compareAndExport(db, msg.GetEnvoyMetrics())
		}
	}
}

func (ir *Receiver) registerMetricsConsumer() error {
	var err = errAlreadyStarted

	ir.startMetricsReceiverOnce.Do(func() {
		srv := ir.grpcServer()
		metricspb.RegisterMetricsServiceServer(srv, ir)
	})
	return err
}

func (ir *Receiver) grpcServer() *grpc.Server {
	ir.mu.Lock()
	defer ir.mu.Unlock()

	if ir.serverGRPC == nil {
		ir.serverGRPC = observability.GRPCServerWithObservabilityEnabled(ir.grpcServerOptions...)
	}

	return ir.serverGRPC
}

// StopMetricsReception is a method to turn off receiving metrics. It
// currently is a noop because we don't yet know if gRPC allows
// stopping a specific service.
func (ir *Receiver) StopMetricsReception(ctx context.Context) error {
	// StopMetricsReception is a noop currently.
	// TODO: (@odeke-em) investigate whether or not gRPC
	// provides a way to stop specific services.
	return nil
}

func (ir *Receiver) startServer() error {
	err := errAlreadyStarted
	ir.startServerOnce.Do(func() {
		errChan := make(chan error, 1)
		go func() {
			errChan <- ir.serverGRPC.Serve(ir.ln)
		}()

		// Our goal is to heuristically try running the server
		// and if it returns an error immediately, we reporter that.
		select {
		case serr := <-errChan:
			err = serr

		case <-time.After(1 * time.Second):
			// No error otherwise returned in the period of 1s.
			// We can assume that the serve is at least running.
			err = nil
		}
	})
	return err
}

func (ir *Receiver) toType(metric *prometheus.MetricFamily) ocmetricspb.MetricDescriptor_Type {
	switch metric.Type {
	case prometheus.MetricType_COUNTER:
		return ocmetricspb.MetricDescriptor_CUMULATIVE_DOUBLE
	case prometheus.MetricType_GAUGE:
		return ocmetricspb.MetricDescriptor_GAUGE_DOUBLE
	case prometheus.MetricType_HISTOGRAM:
		return ocmetricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION
	case prometheus.MetricType_SUMMARY:
		//return ocmetricspb.MetricDescriptor_SUMMARY
		return ocmetricspb.MetricDescriptor_UNSPECIFIED
	default:
		return ocmetricspb.MetricDescriptor_UNSPECIFIED
	}
}

func (ir *Receiver) toLabelKeys(metric *prometheus.MetricFamily) []*ocmetricspb.LabelKey {
	keys := make([]*ocmetricspb.LabelKey, 0, 0)
	m := metric.GetMetric()
	keys = append(keys, &ocmetricspb.LabelKey{Key: "node_id"})
	for _, first := range m {
		// TODO: will the label key pair be same for all Metric?
		labels := first.Label
		for _, label := range labels {
			keys = append(keys, &ocmetricspb.LabelKey{Key: label.Name})
		}
		return keys
	}
	return keys
}

var labelKeySvc = []*ocmetricspb.LabelKey{
	{Key: "direction"},
	{Key: "port"},
	{Key: "protocol"},
	{Key: "service"},
}

func (ir *Receiver) recreateName(mf *prometheus.MetricFamily, mfe *mfEntry) {

	service := ""

	nameIn := mf.GetName()

	//  12 metric=cluster.outbound|9091||istio-telemetry.istio-system.svc.cluster.local.internal.upstream_rq_2xx,
	//  12 metric=cluster.outbound|9091||istio-telemetry.istio-system.svc.cluster.local.upstream_cx_rx_bytes_total,
	// direction | port | protocol | serviceAndMetric
	if strings.Contains(nameIn, "|") {
		parts := strings.Split(nameIn, "|")
		if len(parts) == 4 {
			mfe.rename = true
			serviceAndMetric := parts[3]
			subparts := strings.Split(serviceAndMetric, ".")
			l := len(subparts)
			if l > 1 {
				mfe.name = subparts[len(subparts)-1]
				service = strings.Join(subparts[:(l - 1)], ".")
			} else {
				mfe.name = serviceAndMetric
			}

			mfe.labelKeys = labelKeySvc
			mfe.labelValues = []*ocmetricspb.LabelValue{
				{Value: parts[0], HasValue: true},
				{Value: parts[1], HasValue: true},
				{Value: parts[2], HasValue: true},
				{Value: service, HasValue: true},
			}
		}
	} else if strings.HasPrefix(nameIn, "http") {
		//  1 metric=http.10.24.13.143_9555.downstream_cx_tx_bytes_total,
		//	1 metric=http.10.24.13.143_9555.downstream_rq_2xx,
		parts := strings.Split(nameIn, ".")
		port := ""
		l := len(parts)
		if l == 6 {
			mfe.rename = true
			mfe.name = parts[l-1]
			subpart := strings.Split(parts[4], "_")
			l = len(subpart)
			if l == 2 {
				port = subpart[1]
				service = strings.Join(parts[1:][:3], ".")
				service = fmt.Sprintf("%s.%s", service, subpart[0])
			}
			mfe.labelKeys = labelKeySvc
			mfe.labelValues = []*ocmetricspb.LabelValue{
				{Value: "", HasValue: false},
				{Value: port, HasValue: true},
				{Value: "http", HasValue: true},
				{Value: service, HasValue: true},
			}
		}
	}
}

func (ir *Receiver) toDesc(metric *prometheus.MetricFamily, mfe *mfEntry) *ocmetricspb.MetricDescriptor {

	name := metric.GetName()
	labelKeys := ir.toLabelKeys(metric)

	if mfe != nil && mfe.rename {
		name = mfe.name
		labelKeys = append(mfe.labelKeys, labelKeys...)
	}
	desc := &ocmetricspb.MetricDescriptor{
		Name:        name,
		Description: "",
		Type:        ir.toType(metric),
		LabelKeys:   labelKeys,
	}
	return desc
}

func (ir *Receiver) sumToSum(m *prometheus.Metric) *ocmetricspb.Point_SummaryValue {
	var count uint64
	var sum float64
	if m.Summary.SampleCount > 0 {
		count = m.Summary.SampleCount
		sum = m.Summary.SampleSum
	} else {
		count = 0
		sum = 0
	}
	quantiles := m.Summary.GetQuantile()
	valueAtPercentiles := make([]*ocmetricspb.SummaryValue_Snapshot_ValueAtPercentile, len(quantiles)+1)
	idx := 0
	for _, b := range quantiles {
		valueAtPercentiles[idx] = &ocmetricspb.SummaryValue_Snapshot_ValueAtPercentile{
			Percentile: b.Quantile,
			Value: b.Value,
		}
		idx++
	}
	dv := &ocmetricspb.Point_SummaryValue{
		SummaryValue: &ocmetricspb.SummaryValue{
			Snapshot: &ocmetricspb.SummaryValue_Snapshot{
				PercentileValues: valueAtPercentiles,
			},
			Sum:   &wrappers.DoubleValue{
				Value: sum,
			},
			Count: &wrappers.Int64Value{
				Value: int64(count),
			},
		}}
	return dv
}


func (ir *Receiver) histToDist(m *prometheus.Metric) *ocmetricspb.Point_DistributionValue {
	var count uint64
	var sum float64
	if m.Histogram.SampleCount > 0 {
		count = m.Histogram.SampleCount
		sum = m.Histogram.SampleSum
	} else {
		count = 0
		sum = 0
	}
	var prev *prometheus.Bucket = nil
	buckets := m.Histogram.GetBucket()
	distBuckets := make([]*ocmetricspb.DistributionValue_Bucket, len(buckets)+1)
	bounds := make([]float64, len(buckets))
	idx := 0
	for _, b := range buckets {
		if prev != nil {
			distBuckets[idx] = &ocmetricspb.DistributionValue_Bucket{
				Count: int64(b.CumulativeCount - prev.CumulativeCount),
			}
		} else {
			distBuckets[idx] = &ocmetricspb.DistributionValue_Bucket{
				Count: int64(b.CumulativeCount),
			}
		}
		bounds[idx] = b.UpperBound
		idx++
		prev = b
	}
	if idx > 1 && count > prev.CumulativeCount {
		distBuckets[idx] = &ocmetricspb.DistributionValue_Bucket{
			Count: int64(count - prev.CumulativeCount),
		}
	}
	dv := &ocmetricspb.Point_DistributionValue{
		DistributionValue: &ocmetricspb.DistributionValue{
			Buckets: distBuckets,
			// TODO: Sum of squared deviation.
			Sum:   sum,
			Count: int64(count),
			BucketOptions: &ocmetricspb.DistributionValue_BucketOptions{
				Type: &ocmetricspb.DistributionValue_BucketOptions_Explicit_{
					Explicit: &ocmetricspb.DistributionValue_BucketOptions_Explicit{
						Bounds: bounds,
					},
				},
			},
		}}
	return dv
}

func (ir *Receiver) toPoint(mt prometheus.MetricType, m *prometheus.Metric) ([]*ocmetricspb.Point, error) {
	pts := make([]*ocmetricspb.Point, 0, 0)
	pt := &ocmetricspb.Point{Timestamp: msecToProtoTimestamp(m.TimestampMs)}

	switch mt {
	case prometheus.MetricType_COUNTER:
		pt.Value = &ocmetricspb.Point_DoubleValue{DoubleValue: m.Counter.GetValue()}
	case prometheus.MetricType_GAUGE:
		pt.Value = &ocmetricspb.Point_DoubleValue{DoubleValue: m.Gauge.GetValue()}
	case prometheus.MetricType_HISTOGRAM:
		pt.Value = ir.histToDist(m)
	case prometheus.MetricType_SUMMARY:
		pt.Value = ir.sumToSum(m)
	default:
		return nil, fmt.Errorf("unsupported metric type %v", mt)
	}
	return append(pts, pt), nil
}

func (ir *Receiver) toOneTimeseries(mf *prometheus.MetricFamily, m *prometheus.Metric, startTime int64, nodeId string, mfe *mfEntry) (*ocmetricspb.TimeSeries, error) {
	lv := make([]*ocmetricspb.LabelValue, 0, 0)
	if mfe != nil && mfe.rename {
		lv = append(lv, mfe.labelValues...)
	}
	lv = append(lv, &ocmetricspb.LabelValue{Value: nodeId})
	labels := m.Label
	for _, label := range labels {
		lv = append(lv, &ocmetricspb.LabelValue{Value: label.GetValue()})
	}
	pt, err := ir.toPoint(mf.Type, m)
	if err != nil {
		// TODO: count errors
		return nil, err
	}
	ts := &ocmetricspb.TimeSeries{
		LabelValues:    lv,
		StartTimestamp: msecToProtoTimestamp(startTime),
		Points:         pt,
	}
	return ts, nil
}

func (ir *Receiver) idToNode(n *core.Node) *commonpb.Node {
	// TODD: figure want how to map envoy node to OC Agent node.
	node := &commonpb.Node{
		Identifier:  &commonpb.ProcessIdentifier{HostName: n.Id},
		LibraryInfo: &commonpb.LibraryInfo{},
		ServiceInfo: &commonpb.ServiceInfo{},
	}
	return node
}

// metricSignature creates a unique signature consisting of a
// metric's type and its lexicographically sorted label values
func metricSignature(metric *prometheus.Metric) string {
	labels := metric.Label
	labelValues := make([]string, 0, len(labels))

	for _, label := range labels {
		labelValues = append(labelValues, label.Value)
	}
	sort.Strings(labelValues)
	return fmt.Sprintf("%s", strings.Join(labelValues, ","))
}

func (ir *Receiver) computeDiff(first, curr *prometheus.Metric, metricType prometheus.MetricType) error {

	switch(metricType) {
	case prometheus.MetricType_COUNTER:
		curr.Counter.Value = curr.Counter.Value - first.Counter.Value
	case prometheus.MetricType_HISTOGRAM:
		if len(first.Histogram.Bucket) != len(curr.Histogram.Bucket) {
			// TODO: count errors
			return errHistBucketLenNotEqual
		}
		for i, _ := range curr.Histogram.Bucket {
			if curr.Histogram.Bucket[i].UpperBound != first.Histogram.Bucket[i].UpperBound {
				return errHistBucketLenNotEqual
			}
			// TODO: what if curr value is less than first? Reset
			curr.Histogram.Bucket[i].CumulativeCount = curr.Histogram.Bucket[i].CumulativeCount -
				first.Histogram.Bucket[i].CumulativeCount
		}
		curr.Histogram.SampleSum = curr.Histogram.SampleSum - first.Histogram.SampleSum
		curr.Histogram.SampleCount = curr.Histogram.SampleCount - first.Histogram.SampleCount
	case prometheus.MetricType_SUMMARY:
		curr.Summary.SampleCount = curr.Summary.SampleCount - first.Summary.SampleCount
		curr.Summary.SampleSum = curr.Summary.SampleSum - first.Summary.SampleSum
	default:
	}
	return nil
}

func (ir *Receiver) addOrGetMfe(db *metricsdb, mf *prometheus.MetricFamily) *mfEntry {
	mfe, ok := db.mfes[mf.Name]
	if !ok {
		//save first
		mfe = &mfEntry{mf: mf, metricMap: map[string]*prometheus.Metric{}}
		ir.recreateName(mf, mfe)
		db.mfes[mf.Name] = mfe
	}
	return mfe
}

func (ir *Receiver) toResource(db *metricsdb) *resourcepb.Resource {
	return nil
	//r := &resourcepb.Resource{}
	//r.Labels = map[string]string{}
	//metadata := db.node.Metadata
	//if metadata != nil {
	//	for k, v := range metadata.Fields {
	//		switch k {
	//		case "CONFIG_NAMESPACE":
	//			r.Labels["k8s.namespace.name"] = v.GetStringValue()
	//		case "POD_NAME":
	//			r.Labels["k8s.pod.name"] = v.GetStringValue()
	//		case "app":
	//		}
	//	}
	//
	//}
	//r.Labels["k8s.cluster.name"] = db.node.GetCluster()
	//return r
}
func (ir *Receiver) compareAndExport(db *metricsdb, mfs []*prometheus.MetricFamily) error {
	md := data.MetricsData{Node: ir.idToNode(db.node)}
	ocmetrics := make([]*ocmetricspb.Metric, 0)
	tsCount := 0
	for _, mf := range mfs {
		mfe := ir.addOrGetMfe(db, mf)

		descriptor := ir.toDesc(mf, mfe)
		if descriptor.Type == ocmetricspb.MetricDescriptor_UNSPECIFIED {
			// TODO: [rghetia] Count errors
			log.Printf("unspecified type %v\n", mf)
			continue
		}
		tss := make([]*ocmetricspb.TimeSeries, 0, 0)

		for _, metric := range mf.Metric {
			key := metricSignature(metric)
			first, ok := mfe.metricMap[key]
			if ok {
				// compute diff
				err := ir.computeDiff(first, metric, mf.Type)
				if err != nil {
					// TODO [rghetia] count errors
					log.Printf("computeDiff error: %s-%s %v\n", mf.Name, key, err)
					continue
				}
				ts, err := ir.toOneTimeseries(mf, metric, first.TimestampMs, db.node.Id, mfe)
				if err != nil {
					// TODO [rghetia] count errors
					log.Printf("toOneTimeseries error: %s-%s %v\n", mf.Name, key, err)
					continue
				}
				tss = append(tss, ts)
			} else {
				log.Printf("First occurrence: metric=%s, key=%s, value=%v\n", mfe.mf.GetName(), key, metric)
				mfe.metricMap[key] = metric
			}
		}
		if len(tss) > 0 {
			ocmetric := &ocmetricspb.Metric{
				MetricDescriptor: descriptor,
				Timeseries:       tss,
				Resource:         ir.toResource(db),
			}
			ocmetrics = append(ocmetrics, ocmetric)
			//if mf.Type == prometheus.MetricType_COUNTER {
			//	log.Printf("Counter: envoy: %v\n, oc: %v\n", mf, ocmetric)
			//}
			tsCount += len(tss)
		}
	}
	if len(ocmetrics) > 0 {
		md.Metrics = ocmetrics
		ir.metricsConsumer.ConsumeMetricsData(context.Background(), md)
		log.Printf("Exporting for node:%s, timeseries:%d, metrics:%d\n",
			db.node.Id, tsCount, len(ocmetrics))
	} else {
		log.Printf("Not exporting for node:%s, timeseries:%d, metrics:%d\n",
			db.node.Id, tsCount, len(ocmetrics))
	}
	return nil
}

func msecToProtoTimestamp(ms int64) *timestamp.Timestamp {
	return &timestamp.Timestamp{
		Seconds: int64(ms / 1e3),
		Nanos:   int32((ms % 1e3) * 1e6),
	}
}
