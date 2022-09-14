package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes Driver with prometheus metrics publishing
func Driver(registry prometheus.Registerer, opts ...option) trace.Driver {
	return metrics.Driver(makeConfig(registry, opts...))
}
