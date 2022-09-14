package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with core metrics publishing
func DatabaseSQL(registry prometheus.Registerer, opts ...option) trace.DatabaseSQL {
	return metrics.DatabaseSQL(makeConfig(registry, opts...))
}
