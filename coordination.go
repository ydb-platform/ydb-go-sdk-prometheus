package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Coordination makes trace.Coordination with prometheus metrics publishing
func Coordination(registry prometheus.Registerer, opts ...option) trace.Coordination {
	return metrics.Coordination(makeConfig(registry, opts...))
}
