package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes Driver with solomon metrics publishing
func Driver(registry prometheus.Registerer, opts ...option) trace.Driver {
	c := &config{
		registry:  registry,
		namespace: "ydb_go_sdk",
		separator: "_",
	}
	for _, o := range opts {
		o(c)
	}
	if c.details == 0 {
		c.details = ^metrics.Details(0)
	}
	return metrics.Driver(c)
}
