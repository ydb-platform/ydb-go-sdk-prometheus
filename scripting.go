package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scripting makes trace.Scripting with prometheus metrics publishing
func Scripting(registry prometheus.Registerer, opts ...option) trace.Scripting {
	return metrics.Scripting(makeConfig(registry, opts...))
}
