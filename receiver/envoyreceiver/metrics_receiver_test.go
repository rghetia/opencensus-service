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
	"testing"

	ocmetricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	"github.com/google/go-cmp/cmp"
	prometheus "istio.io/gogo-genproto/prometheus"
	"github.com/golang/protobuf/ptypes/wrappers"
)

func TestMetricToOcMetric(t *testing.T) {
	ir, _ := New("127.0.0.0:55690", nil)
	db := newDb(nil, nil)
	tcs := []struct {
		name   string
		nodeId string
		in     *prometheus.MetricFamily
		want   ocmetricspb.Metric
	}{
		{
			name:   "counter to cumulative",
			nodeId: "n1",
			in: &prometheus.MetricFamily{
				Name: "counter1",
				Type: prometheus.MetricType_COUNTER,
				Metric: []*prometheus.Metric{
					{
						Label: []*prometheus.LabelPair{
							{
								Name:  "k1",
								Value: "v1",
							},
						},
						Gauge: &prometheus.Gauge{},
						Counter: &prometheus.Counter{
							Value: 64.5,
						},
						Summary:     &prometheus.Summary{},
						Untyped:     &prometheus.Untyped{},
						Histogram:   &prometheus.Histogram{},
						TimestampMs: 0,
					},
				},
			},
			want: ocmetricspb.Metric{
				MetricDescriptor: &ocmetricspb.MetricDescriptor{
					Name: "counter1",
					Type: ocmetricspb.MetricDescriptor_CUMULATIVE_DOUBLE,
					LabelKeys: []*ocmetricspb.LabelKey{
						{Key: "node_id"},
						{Key: "k1"},
					},
				},
				Timeseries: []*ocmetricspb.TimeSeries{
					{
						Points: []*ocmetricspb.Point{
							{
								Value:     &ocmetricspb.Point_DoubleValue{64.5},
								Timestamp: msecToProtoTimestamp(0),
							},
						},
						StartTimestamp: msecToProtoTimestamp(0),
						LabelValues: []*ocmetricspb.LabelValue{
							{
								Value: "n1",
							},
							{
								Value: "v1",
							},
						},
					},
				},
			},
		},
		{
			name:   "gauge to guage",
			nodeId: "n2",
			in: &prometheus.MetricFamily{
				Name: "gauge1",
				Type: prometheus.MetricType_GAUGE,
				Metric: []*prometheus.Metric{
					{
						Label: []*prometheus.LabelPair{
							{
								Name:  "k1",
								Value: "v1",
							},
						},
						Gauge: &prometheus.Gauge{
							Value: 55.5,
						},
						Summary:     &prometheus.Summary{},
						Untyped:     &prometheus.Untyped{},
						Histogram:   &prometheus.Histogram{},
						TimestampMs: 0,
					},
				},
			},
			want: ocmetricspb.Metric{
				MetricDescriptor: &ocmetricspb.MetricDescriptor{
					Name: "gauge1",
					Type: ocmetricspb.MetricDescriptor_GAUGE_DOUBLE,
					LabelKeys: []*ocmetricspb.LabelKey{
						{Key: "node_id"},
						{Key: "k1"},
					},
				},
				Timeseries: []*ocmetricspb.TimeSeries{
					{
						Points: []*ocmetricspb.Point{
							{
								Value:     &ocmetricspb.Point_DoubleValue{55.5},
								Timestamp: msecToProtoTimestamp(0),
							},
						},
						StartTimestamp: msecToProtoTimestamp(0),
						LabelValues: []*ocmetricspb.LabelValue{
							{
								Value: "n2",
							},
							{
								Value: "v1",
							},
						},
					},
				},
			},
		},
		{
			name:   "histogram to distribution",
			nodeId: "n3",
			in: &prometheus.MetricFamily{
				Name: "histogram1",
				Type: prometheus.MetricType_HISTOGRAM,
				Metric: []*prometheus.Metric{
					{
						Label: []*prometheus.LabelPair{
							{
								Name:  "k1",
								Value: "v1",
							},
						},
						Histogram: &prometheus.Histogram{
							SampleCount: 3,
							SampleSum:   150.5,
							Bucket: []*prometheus.Bucket{
								{
									CumulativeCount: 1,
									UpperBound:      10.0,
								},
								{
									CumulativeCount: 2,
									UpperBound:      50.0,
								},
							},
						},
						TimestampMs: 0,
					},
				},
			},
			want: ocmetricspb.Metric{
				MetricDescriptor: &ocmetricspb.MetricDescriptor{
					Name: "histogram1",
					Type: ocmetricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION,
					LabelKeys: []*ocmetricspb.LabelKey{
						{Key: "node_id"},
						{Key: "k1"},
					},
				},
				Timeseries: []*ocmetricspb.TimeSeries{
					{
						Points: []*ocmetricspb.Point{
							{
								Value: &ocmetricspb.Point_DistributionValue{
									DistributionValue: &ocmetricspb.DistributionValue{
										Count: 3,
										Sum:   150.5,
										BucketOptions: &ocmetricspb.DistributionValue_BucketOptions{
											Type: &ocmetricspb.DistributionValue_BucketOptions_Explicit_{
												Explicit: &ocmetricspb.DistributionValue_BucketOptions_Explicit{
													Bounds: []float64{
														10.0, 50.0,
													},
												},
											},
										},
										Buckets: []*ocmetricspb.DistributionValue_Bucket{
											{
												Count: 1,
											},
											{
												Count: 1,
											},
											{
												Count: 1,
											},
										},
									},
								},
								Timestamp: msecToProtoTimestamp(0),
							},
						},
						StartTimestamp: msecToProtoTimestamp(0),
						LabelValues: []*ocmetricspb.LabelValue{
							{
								Value: "n3",
							},
							{
								Value: "v1",
							},
						},
					},
				},
			},
		},
		{
			name:   "summary to summary",
			nodeId: "n3",
			in: &prometheus.MetricFamily{
				Name: "summary1",
				Type: prometheus.MetricType_SUMMARY,
				Metric: []*prometheus.Metric{
					{
						Label: []*prometheus.LabelPair{
							{
								Name:  "k1",
								Value: "v1",
							},
						},
						Summary: &prometheus.Summary{
							SampleCount: 3,
							SampleSum:   6,
							Quantile: []*prometheus.Quantile{
								{
									Value:    1,
									Quantile: 0.5,
								},
								{
									Value:    2,
									Quantile: 0.9,
								},
								{
									Value:    3,
									Quantile: 1.0,
								},
							},
						},
						TimestampMs: 0,
					},
				},
			},
			want: ocmetricspb.Metric{
				MetricDescriptor: &ocmetricspb.MetricDescriptor{
					Name: "summary1",
					Type: ocmetricspb.MetricDescriptor_SUMMARY,
					LabelKeys: []*ocmetricspb.LabelKey{
						{Key: "node_id"},
						{Key: "k1"},
					},
				},
				Timeseries: []*ocmetricspb.TimeSeries{
					{
						Points: []*ocmetricspb.Point{
							{
								Value: &ocmetricspb.Point_SummaryValue{
									SummaryValue: &ocmetricspb.SummaryValue{
										Count: &wrappers.Int64Value{
											Value: 3,
										},
										Sum: &wrappers.DoubleValue{
											Value: 6.0,
										},
										Snapshot: &ocmetricspb.SummaryValue_Snapshot{
											PercentileValues: []*ocmetricspb.SummaryValue_Snapshot_ValueAtPercentile{
												{
													Percentile: 0.5,
													Value:      1,
												},
												{
													Percentile: 0.9,
													Value:      2,
												},
												{
													Percentile: 1.0,
													Value:      3,
												},
											},
										},
									},
								},
								Timestamp: msecToProtoTimestamp(0),
							},
						},
						StartTimestamp: msecToProtoTimestamp(0),
						LabelValues: []*ocmetricspb.LabelValue{
							{
								Value: "n3",
							},
							{
								Value: "v1",
							},
						},
					},
				},
			},
		},
		{
			name:   "summary to summary",
			nodeId: "n3",
			in: &prometheus.MetricFamily{
				Name: "cluster.inbound|9555|grpc|adservice.default.svc.cluster.local.upstream_rq_time",
				Type: prometheus.MetricType_SUMMARY,
				Metric: []*prometheus.Metric{
					{
						Label: []*prometheus.LabelPair{
							{
								Name:  "k1",
								Value: "v1",
							},
						},
						Summary: &prometheus.Summary{
							SampleCount: 3,
							SampleSum:   6,
							Quantile: []*prometheus.Quantile{
								{
									Value:    1,
									Quantile: 0.5,
								},
								{
									Value:    2,
									Quantile: 0.9,
								},
								{
									Value:    3,
									Quantile: 1.0,
								},
							},
						},
						TimestampMs: 0,
					},
				},
			},
			want: ocmetricspb.Metric{
				MetricDescriptor: &ocmetricspb.MetricDescriptor{
					Name: "upstream_rq_time",
					Type: ocmetricspb.MetricDescriptor_SUMMARY,
					LabelKeys: []*ocmetricspb.LabelKey{
						{Key: "direction"},
						{Key: "port"},
						{Key: "protocol"},
						{Key: "service"},
						{Key: "origin"},
						{Key: "node_id"},
						{Key: "k1"},
					},
				},
				Timeseries: []*ocmetricspb.TimeSeries{
					{
						Points: []*ocmetricspb.Point{
							{
								Value: &ocmetricspb.Point_SummaryValue{
									SummaryValue: &ocmetricspb.SummaryValue{
										Count: &wrappers.Int64Value{
											Value: 3,
										},
										Sum: &wrappers.DoubleValue{
											Value: 6.0,
										},
										Snapshot: &ocmetricspb.SummaryValue_Snapshot{
											PercentileValues: []*ocmetricspb.SummaryValue_Snapshot_ValueAtPercentile{
												{
													Percentile: 0.5,
													Value:      1,
												},
												{
													Percentile: 0.9,
													Value:      2,
												},
												{
													Percentile: 1.0,
													Value:      3,
												},
											},
										},
									},
								},
								Timestamp: msecToProtoTimestamp(0),
							},
						},
						StartTimestamp: msecToProtoTimestamp(0),
						LabelValues: []*ocmetricspb.LabelValue{
							{Value: "inbound", HasValue: true},
							{Value: "9555", HasValue: true},
							{Value: "grpc", HasValue: true},
							{Value: "adservice.default", HasValue: true},
							{Value: "", HasValue: false},
							{
								Value: "n3",
							},
							{
								Value: "v1",
							},
						},
					},
				},
			},
		},
	}
	for _, tc := range tcs {
		mfe := ir.addOrGetMfe(db, tc.in)
		gotDesc := ir.toDesc(tc.in, mfe)
		if !cmp.Equal(gotDesc, tc.want.MetricDescriptor) {
			t.Fatalf("test descriptor %s:\n got=%v\n want=%v\n", tc.name, *gotDesc, tc.want.MetricDescriptor)
		}
		for i, metric := range tc.in.Metric {
			gotTs, err := ir.toOneTimeseries(tc.in, metric, 0, tc.nodeId, mfe)
			if err != nil {
				t.Fatalf("test %s failed with error %v", tc.name, err)
			}
			wantTs := tc.want.Timeseries[i]
			if !cmp.Equal(gotTs, tc.want.Timeseries[i]) {
				t.Fatalf("test timeseries %s:\n got=%v\n want=%v\n", tc.name, gotTs, wantTs)
			}
		}
	}
}

func TestExtractName(t *testing.T) {
	ir, _ := New("127.0.0.0:55690", nil)
	tcs := []struct {
		nameIn  string
		wantMfe *mfEntry
	}{
		{nameIn: "cluster.inbound|9555|grpc|adservice.default.svc.cluster.local.upstream_rq_time",
			wantMfe: &mfEntry{
				renamed: true,
				name:    "upstream_rq_time",
				labelKeys: labelKeySvc,
				labelValues: []*ocmetricspb.LabelValue{
					{Value: "inbound", HasValue: true},
					{Value: "9555", HasValue: true},
					{Value: "grpc", HasValue: true},
					{Value: "adservice.default", HasValue: true},
					{Value: "", HasValue: false},
				},
			},
		},
		{nameIn: "cluster.inbound|9555|grpc|adservice.default.svc.cluster.local.external.upstream_rq_time",
			wantMfe: &mfEntry{
				renamed: true,
				name:    "upstream_rq_time",
				labelKeys: labelKeySvc,
				labelValues: []*ocmetricspb.LabelValue{
					{Value: "inbound", HasValue: true},
					{Value: "9555", HasValue: true},
					{Value: "grpc", HasValue: true},
					{Value: "adservice.default", HasValue: true},
					{Value: "external", HasValue: true},
				},
			},
		},
		{nameIn: "cluster.outbound|9555||adservice.default.svc.cluster.local.external.upstream_rq_time",
			wantMfe: &mfEntry{
				renamed: true,
				name:    "upstream_rq_time",
				labelKeys: labelKeySvc,
				labelValues: []*ocmetricspb.LabelValue{
					{Value: "outbound", HasValue: true},
					{Value: "9555", HasValue: true},
					{Value: "", HasValue: false},
					{Value: "adservice.default", HasValue: true},
					{Value: "external", HasValue: true},
				},
			},
		},
		{nameIn: "cluster.outbound|9555||adservice.default.svc.cluster.local.upstream_rq_time",
			wantMfe: &mfEntry{
				renamed: true,
				name:    "upstream_rq_time",
				labelKeys: labelKeySvc,
				labelValues: []*ocmetricspb.LabelValue{
					{Value: "outbound", HasValue: true},
					{Value: "9555", HasValue: true},
					{Value: "", HasValue: false},
					{Value: "adservice.default", HasValue: true},
					{Value: "", HasValue: false},
				},
			},
		},
		{nameIn: "cluster.outbound|9091||istio-telemetry.istio-system.svc.cluster.local.zone.us-central1-b.us-central1-c.upstream_rq_time",
			wantMfe: &mfEntry{
				renamed: true,
				name:    "upstream_rq_time",
				labelKeys: labelKeySvc,
				labelValues: []*ocmetricspb.LabelValue{
					{Value: "outbound", HasValue: true},
					{Value: "9091", HasValue: true},
					{Value: "", HasValue: false},
					{Value: "istio-telemetry.istio-system", HasValue: true},
					{Value: "zone.us-central1-b.us-central1-c", HasValue: true},
				},
			},
		},
		{nameIn: "cluster.prometheus_stats.external.upstream_rq_time",
			wantMfe: &mfEntry{
				renamed: true,
				name:    "upstream_rq_time",
				labelKeys: labelKeySvc,
				labelValues: []*ocmetricspb.LabelValue{
					{Value: "", HasValue: false},
					{Value: "", HasValue: false},
					{Value: "", HasValue: false},
					{Value: "prometheus_stats", HasValue: true},
					{Value: "external", HasValue: true},
				},
			},
		},
		{nameIn: "cluster.prometheus_stats.upstream_rq_time",
			wantMfe: &mfEntry{
				renamed: true,
				name:    "upstream_rq_time",
				labelKeys: labelKeySvc,
				labelValues: []*ocmetricspb.LabelValue{
					{Value: "", HasValue: false},
					{Value: "", HasValue: false},
					{Value: "", HasValue: false},
					{Value: "prometheus_stats", HasValue: true},
					{Value: "", HasValue: false},
				},
			},
		},
		{nameIn: "cluster.xds-grpc.upstream_rq_2xx",
			wantMfe: &mfEntry{
				renamed: true,
				name:    "upstream_rq_2xx",
				labelKeys: labelKeySvc,
				labelValues: []*ocmetricspb.LabelValue{
					{Value: "", HasValue: false},
					{Value: "", HasValue: false},
					{Value: "", HasValue: false},
					{Value: "xds-grpc", HasValue: true},
					{Value: "", HasValue: false},
				},
			},
		},
		{nameIn: "http.10.24.10.187_7070.downstream_cx_active",
			wantMfe: &mfEntry{
				renamed: true,
				name:    "downstream_cx_active",
				labelKeys: labelKeySvc,
				labelValues: []*ocmetricspb.LabelValue{
					{Value: "", HasValue: false},
					{Value: "7070", HasValue: true},
					{Value: "http", HasValue: true},
					{Value: "10.24.10.187", HasValue: true},
					{Value: "", HasValue: false},
				},
			},
		},
		{nameIn: "cluster.outbound|80||metadata.google.internal.external.upstream_rq_time",
			wantMfe: &mfEntry{
				renamed: true,
				name:    "upstream_rq_time",
				labelKeys: labelKeySvc,
				labelValues: []*ocmetricspb.LabelValue{
					{Value: "outbound", HasValue: true},
					{Value: "80", HasValue: true},
					{Value: "", HasValue: false},
					{Value: "metadata.google.internal", HasValue: true},
					{Value: "external", HasValue: true},
				},
			},
		},
		{nameIn: "junk.service.name",
			wantMfe: &mfEntry{
				renamed: false,
			},
		},
	}
	for _, tc := range tcs {
		mfe := &mfEntry{}
		ir.recreateName(tc.nameIn, mfe)
		if !cmp.Equal(mfe, tc.wantMfe, cmp.AllowUnexported(mfEntry{})) {
			t.Fatalf("test metric %s:\n got=%v\n want=%v\n", tc.nameIn, mfe, tc.wantMfe)
		}
	}
}
