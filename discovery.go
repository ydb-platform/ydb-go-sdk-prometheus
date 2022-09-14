package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Discovery makes trace.Discovery with prometheus metrics publishing
func Discovery(registry prometheus.Registerer, opts ...option) trace.Discovery {
	return metrics.Discovery(makeConfig(registry, opts...))
}
