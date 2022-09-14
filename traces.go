package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	metrics "github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func WithTraces(registry prometheus.Registerer, opts ...option) ydb.Option {
	return metrics.WithTraces(makeConfig(registry, opts...))
}
