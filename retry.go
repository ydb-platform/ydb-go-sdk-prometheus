package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Retry makes trace.Retry with prometheus metrics publishing
func Retry(registry prometheus.Registerer, opts ...option) trace.Retry {
	return metrics.Retry(makeConfig(registry, opts...))
}
