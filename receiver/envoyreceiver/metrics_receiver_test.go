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
)

func TestMetricToOcMetric(t *testing.T) {
	ir, _ := New("127.0.0.0:55690", nil)
	tcs := []struct {
		name string
		in   prometheus.MetricFamily
		want ocmetricspb.Metric
	}{
		{
			name: "counter to cumulative",
			in: prometheus.MetricFamily{
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
								Value: "v1",
							},
						},
					},
				},
			},
		},
		{
			name: "counter to cumulative",
			in: prometheus.MetricFamily{
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
								Value: "v1",
							},
						},
					},
				},
			},
		},
		{
			name: "counter to cumulative",
			in: prometheus.MetricFamily{
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
								Value: "v1",
							},
						},
					},
				},
			},
		},
	}
	for _, tc := range tcs {
		got, err := ir.metricToOCMetric(&tc.in, msecToProtoTimestamp(0))
		if err != nil {
			t.Fatalf("test %s failed with error %v", tc.name, err)
		}
		if !cmp.Equal(*got, tc.want) {
			t.Fatalf("test %s:\n got=%v\n want=%v\n", tc.name, *got, tc.want)
		}
	}
}
