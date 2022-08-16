package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	metrics "github.com/ydb-platform/ydb-go-sdk-metrics"
)

func WithTraces(registry prometheus.Registerer, opts ...option) ydb.Option {
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
	return metrics.WithTraces(c)
}
