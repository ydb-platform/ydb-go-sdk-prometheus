package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes table.ClientTrace with solomon metrics publishing
func Table(registry prometheus.Registerer, opts ...option) trace.Table {
	c := &config{
		registry:  registry,
		namespace: defaultNamespace,
		separator: defaultSeparator,
	}
	for _, o := range opts {
		o(c)
	}

	if c.details == 0 {
		c.details = ^metrics.Details(0)
	}
	return metrics.Table(c)
}
