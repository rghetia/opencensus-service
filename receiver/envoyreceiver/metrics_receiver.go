// Copyright 2018, OpenCensus Authors
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
	"net"
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
	"google.golang.org/grpc/peer"
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
	startTime *timestamp.Timestamp
	mf   map[string]*prometheus.MetricFamily
}

type metricProtoPayload struct {
	ctx        context.Context
	clientAddr string
	msg        *metricspb.StreamMetricsMessage
}

var (
	errAlreadyStarted = errors.New("already started")
	errAlreadyStopped = errors.New("already stopped")
)

const (
	defaultReportingDuration    = 60 * time.Second
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

func (ir *Receiver) export() {
	// First copy
	ir.dbMu.Lock()
	defer ir.dbMu.Unlock()

	for id, db := range ir.db {
		fmt.Printf("Exporting metrics for node %s: count %d\n", id, len(db.mf))
		md := data.MetricsData{Node: ir.idToNode(db.node)}
		prev := db.mf
		db.mf = map[string]*prometheus.MetricFamily{}
		for _, metric := range prev {
			m, err := ir.metricToOCMetric(metric, db.startTime, db.node.Id)
			if err != nil {
				// TODO: count errors
				continue
			}
			fmt.Printf("Metrics: %v\n", m)
			md.Metrics = append(md.Metrics, m)
		}
		ir.metricsConsumer.ConsumeMetricsData(context.Background(), md)
	}
}

func (ir *Receiver) startInternal() {
	for {
		select {
		case <-ir.timer.C:
			ir.export()
		case <-ir.quit:
			ir.timer.Stop()
			ir.done <- true
			return
		}
	}
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
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&metricspb.StreamMetricsResponse{})
		}
		pr, ok := peer.FromContext(stream.Context())
		var clientAddr string
		if ok {
			clientAddr = pr.Addr.String()
		} else {
			clientAddr = "unknown"
		}

		payload := &metricProtoPayload{
			ctx:        stream.Context(),
			clientAddr: clientAddr,
			msg:        msg,
		}
		ir.protoMetricsBundler.Add(payload, 1)
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

			//Start export timer.
			ir.timer = time.NewTicker(defaultReportingDuration)
			ir.quit = make(chan bool)
			ir.done = make(chan bool)

			ir.startInternal()
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
	default:
		// TODO: add support for summary type
		return nil, fmt.Errorf("unsupported metric type %v", mt)
	}
	return append(pts, pt), nil
}

func (ir *Receiver) toTimeseries(metric *prometheus.MetricFamily, startTime *timestamp.Timestamp, nodeId string) []*ocmetricspb.TimeSeries {

	tss := make([]*ocmetricspb.TimeSeries, 0, 0)
	mSlice := metric.GetMetric()
	for _, m := range mSlice {
		lv := make([]*ocmetricspb.LabelValue, 0, 0)
		lv = append(lv, &ocmetricspb.LabelValue{Value: nodeId})
		labels := m.Label
		for _, label := range labels {
			lv = append(lv, &ocmetricspb.LabelValue{Value: label.GetValue()})
		}
		pt, err := ir.toPoint(metric.Type, m)
		if err != nil {
			// TODO: count errors
			continue
		}
		ts := &ocmetricspb.TimeSeries{
			LabelValues:    lv,
			StartTimestamp: startTime,
			Points:         pt,
		}
		tss = append(tss, ts)
	}
	return tss
}

func (ir *Receiver) metricToOCMetric(metric *prometheus.MetricFamily, startTime *timestamp.Timestamp, nodeId string) (*ocmetricspb.Metric, error) {

	descriptor := ir.toDesc(metric)
	if descriptor.Type == ocmetricspb.MetricDescriptor_UNSPECIFIED {
		return nil, fmt.Errorf("descriptor type unspecified %v", descriptor)
	}
	timeseries := ir.toTimeseries(metric, startTime, nodeId)

	ocmetric := &ocmetricspb.Metric{
		MetricDescriptor: descriptor,
		Timeseries:       timeseries,
	}
	return ocmetric, nil
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
	ir.nodeMap[clientAddr] = node
}

func (ir *Receiver) getNodeID(clientAddr string) *core.Node {
	id, ok := ir.nodeMap[clientAddr]
	if ok {
		return id
	}
	return nil
}

func (ir *Receiver) storeMetric(node *core.Node, metric *prometheus.MetricFamily) {
	db, ok := ir.db[node.Id]
	if ok {
		db.mf[metric.Name] = metric
	} else {
		now := time.Now()
		ir.db[node.Id] = metricsdb{
			node: node,
			startTime: &timestamp.Timestamp{
				Seconds: now.Unix(),
				Nanos: int32(now.Nanosecond()),
			},
			mf: map[string]*prometheus.MetricFamily{}}
	}
}

func (ir *Receiver) processStreamMessage(payload *metricProtoPayload) {
	ir.dbMu.Lock()
	defer ir.dbMu.Unlock()

	id := payload.msg.GetIdentifier()
	if id != nil && id.Node != nil {
		ir.addNodeID(payload.clientAddr, id.Node)
	}

	node := ir.getNodeID(payload.clientAddr)
	if node == nil {
		// should never happen
		fmt.Errorf("Received metrics without node info from %s", payload.clientAddr)
		return
	}
	metrics := payload.msg.GetEnvoyMetrics()
	for _, metric := range metrics {
		ir.storeMetric(node, metric)
	}
}

func msecToProtoTimestamp(ms int64) *timestamp.Timestamp {
	return &timestamp.Timestamp{
		Seconds: int64(ms / 1e3),
		Nanos:   int32((ms % 1e3) * 1e6),
	}
}
