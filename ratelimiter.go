package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Ratelimiter makes trace.Ratelimiter with prometheus metrics publishing
func Ratelimiter(registry prometheus.Registerer, opts ...option) trace.Ratelimiter {
	return metrics.Ratelimiter(makeConfig(registry, opts...))
}
