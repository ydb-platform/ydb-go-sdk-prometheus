package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Coordination makes trace.Coordination with prometheus metrics publishing
func Coordination(registry prometheus.Registerer, opts ...option) trace.Coordination {
	c := &config{
		registry:  registry,
		namespace: defaultNamespace,
		separator: defaultSeparator,
	}
	for _, o := range opts {
		o(c)
	}
	if c.details == 0 {
		c.details = trace.DetailsAll
	}
	return metrics.Coordination(c)
}
