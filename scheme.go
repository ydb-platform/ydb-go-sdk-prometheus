package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scheme makes trace.Scheme with prometheus metrics publishing
func Scheme(registry prometheus.Registerer, opts ...option) trace.Scheme {
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
	return metrics.Scheme(c)
}
