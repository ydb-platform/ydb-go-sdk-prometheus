package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/metrics"
)

func WithTraces(registry prometheus.Registerer, opts ...option) ydb.Option {
	return metrics.WithTraces(Config(registry, opts...))
}
