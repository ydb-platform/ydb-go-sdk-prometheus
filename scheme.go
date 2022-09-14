package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scheme makes trace.Scheme with prometheus metrics publishing
func Scheme(registry prometheus.Registerer, opts ...option) trace.Scheme {
	return metrics.Scheme(makeConfig(registry, opts...))
}
