package starttimeattribute

import (
	"context"
	"fmt"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterset"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/k8sconfig"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/selection"
)

const (
	// Type is the value users can use to configure the start time metric adjuster.
	Type = "starttimeattribute"
)

type Adjuster struct {
	apiConfig k8sconfig.APIConfig
	podClient podClient
	logger    *zap.Logger

	filter filterset.FilterSet
}

type podIdentifier struct {
	Value string
	Type  idType
}

type idType byte

const (
	podIP idType = iota
	podName
	podUID
)

type podClient interface {
	GetPodStartTime(ctx context.Context, podID podIdentifier) (time.Time, error)
}

type informerFilter struct {
	Namespace    string
	FieldFilters []FieldFilter
	LabelFilters []LabelFilter
}

type FieldFilter struct {
	Key   string
	Value string
	Op    selection.Operator
}

type LabelFilter struct {
	Key   string
	Value string
	Op    selection.Operator
}

type podClientFactory func(context.Context, k8sconfig.APIConfig, informerFilter) (podClient, error)

// NewAdjuster returns a new Adjuster which adjust metrics' start times based on the initial received points.
func NewAdjuster(set component.TelemetrySettings, filter filterset.FilterSet) (*Adjuster, error) {
	return NewAdjusterWithFactory(set, newK8sPodClient, filter)
}

// NewAdjusterWithFactory returns a new Adjuster with a custom pod client factory
func NewAdjusterWithFactory(set component.TelemetrySettings, factory podClientFactory, filter filterset.FilterSet) (*Adjuster, error) {
	apiConfig := k8sconfig.APIConfig{
		AuthType: k8sconfig.AuthTypeServiceAccount,
	}

	ctx := context.Background()
	// TODO: pass in informer filters
	client, err := factory(ctx, apiConfig, informerFilter{})
	if err != nil {
		return nil, fmt.Errorf("failed to create pod client: %w", err)
	}

	return &Adjuster{
		apiConfig: apiConfig,
		podClient: client,
		logger:    set.Logger,
		filter:    filter,
	}, nil
}

func (a *Adjuster) AdjustMetrics(ctx context.Context, metrics pmetric.Metrics) (pmetric.Metrics, error) {
	resourceMetrics := metrics.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		rm := resourceMetrics.At(i)
		resource := rm.Resource()
		// Try to extract pod identifier from resource attributes
		podID := a.extractPodIdentifier(resource.Attributes())
		if podID == nil {
			continue
		}

		scopeMetrics := rm.ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			sm := scopeMetrics.At(j)
			metrics := sm.Metrics()

			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)

				metricName := metric.Name()
				// Only process cumulative metrics
				if !a.isCumulativeMetric(metric) {
					a.logger.Debug("metric is not cumulative, skipping",
						zap.String("metricName", metricName))
					continue
				}
				if !a.filter.Matches(metricName) {
					a.logger.Debug("metric not included by filter, skipper",
						zap.String("metricName", metricName))
					continue
				}
				// Get pod start time
				startTime, err := a.podClient.GetPodStartTime(ctx, *podID)
				if err != nil {
					a.logger.Debug("Failed to get pod start time for metric",
						zap.String("pod_id", podID.Value),
						zap.String("metricName", metricName),
						zap.Error(err))
					continue
				}

				// Set start time for all data points
				a.setStartTimeForMetric(metric, startTime)
			}
		}
	}

	return metrics, nil
}

func (a *Adjuster) extractPodIdentifier(attrs pcommon.Map) *podIdentifier {
	// Check for pod IP
	if ipVal, ok := attrs.Get("k8s.pod.ip"); ok {
		return &podIdentifier{
			Value: ipVal.AsString(),
			Type:  podIP,
		}
	}

	// Check for pod name with namespace
	podNameVal, nameOk := attrs.Get("k8s.pod.name")
	namespaceVal, nsOk := attrs.Get("k8s.namespace.name")
	if nameOk && nsOk {
		return &podIdentifier{
			Value: fmt.Sprintf("%s/%s", namespaceVal.AsString(), podNameVal.AsString()),
			Type:  podName,
		}
	}

	// Check for pod UID
	if uidVal, ok := attrs.Get("k8s.pod.uid"); ok {
		return &podIdentifier{
			Value: uidVal.AsString(),
			Type:  podUID,
		}
	}

	return nil
}

func (a *Adjuster) isCumulativeMetric(metric pmetric.Metric) bool {
	switch metric.Type() {
	case pmetric.MetricTypeSum:
		return metric.Sum().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	case pmetric.MetricTypeHistogram:
		return metric.Histogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	case pmetric.MetricTypeExponentialHistogram:
		return metric.ExponentialHistogram().AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	default:
		return false
	}
}

func (a *Adjuster) setStartTimeForMetric(metric pmetric.Metric, startTime time.Time) {
	startTimeNanos := pcommon.NewTimestampFromTime(startTime)

	switch metric.Type() {
	case pmetric.MetricTypeSum:
		dataPoints := metric.Sum().DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dataPoints.At(i).SetStartTimestamp(startTimeNanos)
		}
	case pmetric.MetricTypeHistogram:
		dataPoints := metric.Histogram().DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dataPoints.At(i).SetStartTimestamp(startTimeNanos)
		}
	case pmetric.MetricTypeExponentialHistogram:
		dataPoints := metric.ExponentialHistogram().DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dataPoints.At(i).SetStartTimestamp(startTimeNanos)
		}
	}
}
