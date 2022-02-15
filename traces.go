package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	metrics "github.com/ydb-platform/ydb-go-sdk-metrics"
)

func WithTraces(registry prometheus.Registerer, opts ...option) ydb.Option {
	c := &config{
		registry:  registry,
		namespace: defaultNamespace,
		separator: defaultSeparator,
	}
	for _, o := range opts {
		o(c)
	}
	if c.details == 0 {
		c.details = trace.DetailsAll
	}
	return ydb.MergeOptions(
		ydb.WithTraceDriver(metrics.Driver(c)),
		ydb.WithTraceTable(metrics.Table(c)),
		ydb.WithTraceScripting(metrics.Scripting(c)),
		ydb.WithTraceScheme(metrics.Scheme(c)),
		ydb.WithTraceCoordination(metrics.Coordination(c)),
		ydb.WithTraceRatelimiter(metrics.Ratelimiter(c)),
		ydb.WithTraceDiscovery(metrics.Discovery(c)),
	)
}
