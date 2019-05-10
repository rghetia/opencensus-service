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
	"github.com/census-instrumentation/opencensus-service/data"
	"github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	metricspb "github.com/envoyproxy/go-control-plane/envoy/service/metrics/v2"
	"github.com/golang/protobuf/ptypes/timestamp"
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

	nodeMap map[string]*core.Node
	db      map[string]metricsdb
	dbMu    sync.RWMutex

	timer      *time.Ticker
	quit, done chan bool
}

type metricsdb struct {
	node *core.Node
	mfes map[string]*mfEntry
}

type mfEntry struct {
	mf        *prometheus.MetricFamily
	metricMap map[string]*prometheus.Metric
}

type metricProtoPayload struct {
	ctx        context.Context
	clientAddr string
	msg        *metricspb.StreamMetricsMessage
}

var (
	errAlreadyStarted          = errors.New("already started")
	errAlreadyStopped          = errors.New("already stopped")
	errHistBucketLenNotEqual   = errors.New("histogram bucket length not equal")
	errHistBucketBoundNotEqual = errors.New("histogram bucket bound not equal")
)

const (
	source                      = "EnvoyReceiver"
	defaultAddr                 = ":55700"
	defaultBundleCountThreshold = 300
	defaultBundleDelayThreshold = 10
)

// New just creates the Istio receiver services. It is the caller's
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

	ir.protoMetricsBundler = bundler.NewBundler((*metricProtoPayload)(nil), func(bundle interface{}) {
		payloads := bundle.([]*metricProtoPayload)
		ir.handleStreamMetricMessage(payloads)
	})
	// TODO: provide option to set bundle options.
	ir.protoMetricsBundler.BundleCountThreshold = defaultBundleCountThreshold
	ir.protoMetricsBundler.DelayThreshold = defaultBundleDelayThreshold

	ir.db = map[string]metricsdb{}
	ir.nodeMap = map[string]*core.Node{}

	return ir, nil
}

// MetricsSource returns the name of the metrics data source.
func (ir *Receiver) MetricsSource() string {
	return source
}

func (ir *Receiver) handleStreamMetricMessage(payloads []*metricProtoPayload) {
	for _, payload := range payloads {
		ir.processStreamMessage(payload)
	}

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
		//pr, ok := peer.FromContext(stream.Context())
		//var clientAddr string
		//if ok {
		//	clientAddr = pr.Addr.String()
		//} else {
		//	clientAddr = "unknown"
		//}

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
		//payload := &metricProtoPayload{
		//	ctx:        stream.Context(),
		//	clientAddr: clientAddr,
		//	msg:        msg,
		//}
		//ir.protoMetricsBundler.Add(payload, 1)
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

	if ir.quit == nil {
		return nil
	}
	ir.quit <- true
	<-ir.done
	close(ir.quit)
	close(ir.done)
	ir.quit = nil
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
		//TODO: when summary is supported.
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

func (ir *Receiver) toDesc(metric *prometheus.MetricFamily) *ocmetricspb.MetricDescriptor {

	desc := &ocmetricspb.MetricDescriptor{
		Name:        metric.GetName(),
		Description: "",
		Type:        ir.toType(metric),
		LabelKeys:   ir.toLabelKeys(metric),
	}
	return desc
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
		// TODO: add support for summary type
		return nil, fmt.Errorf("summary %v", m)
	default:
		return nil, fmt.Errorf("unsupported metric type %v", mt)
	}
	return append(pts, pt), nil
}

func (ir *Receiver) toOneTimeseries(mf *prometheus.MetricFamily, m *prometheus.Metric, startTime int64, nodeId string) (*ocmetricspb.TimeSeries, error) {
	lv := make([]*ocmetricspb.LabelValue, 0, 0)
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
	// TODD: figure want how to map istio node to OC Agent node.
	node := &commonpb.Node{
		Identifier:  &commonpb.ProcessIdentifier{HostName: n.Id},
		LibraryInfo: &commonpb.LibraryInfo{},
		ServiceInfo: &commonpb.ServiceInfo{},
	}
	return node
}

func (ir *Receiver) addNodeID(clientAddr string, node *core.Node) {
	//ir.nodeMap[clientAddr] = node
	_, ok := ir.db[node.Id]
	if !ok {
		ir.db[node.Id] = metricsdb{
			node: node,
			mfes: map[string]*mfEntry{},
		}
	}
}

func (ir *Receiver) getNodeID(clientAddr string) *core.Node {
	id, ok := ir.nodeMap[clientAddr]
	if ok {
		return id
	}
	return nil
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
	default:
	}
	return nil
}

func (ir *Receiver) addOrGetMfe(db *metricsdb, mf *prometheus.MetricFamily) *mfEntry {
	mfe, ok := db.mfes[mf.Name]
	if !ok {
		//save first
		mfe = &mfEntry{mf: mf, metricMap: map[string]*prometheus.Metric{}}
		db.mfes[mf.Name] = mfe
	}
	return mfe
}

func (ir *Receiver) compareAndExport(db *metricsdb, mfs []*prometheus.MetricFamily) error {
	md := data.MetricsData{Node: ir.idToNode(db.node)}
	ocmetrics := make([]*ocmetricspb.Metric, 0)
	tsCount := 0
	for _, mf := range mfs {
		mfe := ir.addOrGetMfe(db, mf)

		descriptor := ir.toDesc(mf)
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
				ts, err := ir.toOneTimeseries(mf, metric, first.TimestampMs, db.node.Id)
				if err != nil {
					// TODO [rghetia] count errors
					log.Printf("toOneTimeseries error: %s-%s %v\n", mf.Name, key, err)
					continue
				}
				tss = append(tss, ts)
			} else {
				mfe.metricMap[key] = metric
			}
		}
		if len(tss) > 0 {
			ocmetric := &ocmetricspb.Metric{
				MetricDescriptor: descriptor,
				Timeseries:       tss,
			}
			ocmetrics = append(ocmetrics, ocmetric)
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

func (ir *Receiver) processStreamMessage(payload *metricProtoPayload) {
	//ir.dbMu.Lock()
	//defer ir.dbMu.Unlock()
	//
	//id := payload.msg.GetIdentifier()
	//if id != nil && id.Node != nil {
	//	ir.addNodeID(payload.clientAddr, id.Node)
	//}
	//
	//node := ir.getNodeID(payload.clientAddr)
	//if node == nil {
	//	// should never happen
	//	fmt.Errorf("Received metrics without node info from %s", payload.clientAddr)
	//	return
	//}
	//metrics := payload.msg.GetEnvoyMetrics()
	//ir.compareAndExport(node, metrics)
}

func msecToProtoTimestamp(ms int64) *timestamp.Timestamp {
	return &timestamp.Timestamp{
		Seconds: int64(ms / 1e3),
		Nanos:   int32((ms % 1e3) * 1e6),
	}
}
