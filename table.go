package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes trace.Trace with prometheus metrics publishing
func Table(registry prometheus.Registerer, opts ...option) trace.Table {
	return metrics.Table(makeConfig(registry, opts...))
}
