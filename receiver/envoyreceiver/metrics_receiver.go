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
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/census-instrumentation/opencensus-service/consumer"
	"github.com/census-instrumentation/opencensus-service/observability"
	metricspb "github.com/envoyproxy/go-control-plane/envoy/service/metrics/v2"
	ocmetricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	"io"
	prometheus "istio.io/gogo-genproto/prometheus"
	"github.com/census-instrumentation/opencensus-service/data"
	"github.com/golang/protobuf/ptypes/timestamp"
	"google.golang.org/grpc/peer"
)

// Receiver is the type that exposes Trace and Metrics reception.
type Receiver struct {
	mu                sync.Mutex
	ln                net.Listener
	serverGRPC        *grpc.Server
	grpcServerOptions []grpc.ServerOption

	metricsConsumer consumer.MetricsConsumer

	stopOnce                 sync.Once
	startServerOnce          sync.Once
	startMetricsReceiverOnce sync.Once
}

var (
	errAlreadyStarted = errors.New("already started")
	errAlreadyStopped = errors.New("already stopped")
)

const source string = "EnvoyReceiver"
const defaultAddr string = ":55700"

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
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&metricspb.StreamMetricsResponse{
			})
		}
		fmt.Printf("%v\n", msg)
		// TODO: process message.
		ir.processStreamMessage(stream.Context(), msg)
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
		//TODO: when summary is supported.
		return ocmetricspb.MetricDescriptor_UNSPECIFIED
	default:
		return ocmetricspb.MetricDescriptor_UNSPECIFIED
	}
}

func (ir *Receiver) toLabelKeys(metric *prometheus.MetricFamily) []*ocmetricspb.LabelKey {
	keys := make([]*ocmetricspb.LabelKey, 0, 0)
	m := metric.GetMetric()
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
	pt := &ocmetricspb.Point{Timestamp: timeToProtoTimestamp(m.TimestampMs)}

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

func (ir *Receiver) toTimeseries(metric *prometheus.MetricFamily) []*ocmetricspb.TimeSeries {

	tss := make([]*ocmetricspb.TimeSeries, 0, 0)
	mSlice := metric.GetMetric()
	for _, m := range mSlice {
		lv := make([]*ocmetricspb.LabelValue, 0, 0)
		labels := m.Label
		for _, label := range labels {
			lv = append(lv, &ocmetricspb.LabelValue{Value: label.GetValue()})
		}
		pt, err := ir.toPoint(metric.Type, m)
		if err != nil {
			// TODO: count errors
			continue;
		}
		ts := &ocmetricspb.TimeSeries{
			LabelValues:    lv,
			StartTimestamp: timeToProtoTimestamp(m.TimestampMs),
			Points:         pt,
		}
		tss = append(tss, ts)
	}
	return tss
}

func (ir *Receiver) metricToOCMetric(metric *prometheus.MetricFamily) (*ocmetricspb.Metric, error) {

	descriptor := ir.toDesc(metric)
	timeseries := ir.toTimeseries(metric)

	ocmetric := &ocmetricspb.Metric{
		MetricDescriptor: descriptor,
		Timeseries:       timeseries,
	}
	return ocmetric, nil
}

func (ir *Receiver) idToNode(ctx context.Context, id *metricspb.StreamMetricsMessage_Identifier) *commonpb.Node {
	// TODD: figure want how to map istio node to OC Agent node.
	hostname := "nohost"
	if id != nil && id.Node != nil {
		hostname = id.Node.Id
	} else {
		pr, ok := peer.FromContext(ctx)
		if ok {
			hostname = pr.Addr.String()
		}
	}
	node := &commonpb.Node{
		Identifier:  &commonpb.ProcessIdentifier{HostName: hostname},
		LibraryInfo: &commonpb.LibraryInfo{},
		ServiceInfo: &commonpb.ServiceInfo{},
	}
	return node
}

func (ir *Receiver) processStreamMessage(ctx context.Context, msg *metricspb.StreamMetricsMessage) {
	md := data.MetricsData{Node: ir.idToNode(ctx, msg.GetIdentifier())}
	metrics := msg.GetEnvoyMetrics()
	for _, metric := range metrics {
		m, err := ir.metricToOCMetric(metric)
		if err != nil {
			// TODO: count errors
			continue
		}
		md.Metrics = append(md.Metrics, m)
	}
	ir.metricsConsumer.ConsumeMetricsData(context.Background(), md)
}

func timeToProtoTimestamp(ms int64) *timestamp.Timestamp {
	return &timestamp.Timestamp{
		Seconds: int64(ms / 1e3),
		Nanos:   int32((ms % 1e3) * 1e6),
	}
}
