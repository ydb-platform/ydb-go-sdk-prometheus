package metrics

import (
	metrics "github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with core metrics publishing
func DatabaseSQL(registry coreMetrics.Registry, opts ...option) trace.DatabaseSQL {
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
	return metrics.DatabaseSQL(c)
}
