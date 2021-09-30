package go_metrics

import (
	"github.com/rcrowley/go-metrics"
	"github.com/ydb-platform/ydb-go-sdk-metrics-go-metrics/internal/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes Driver with solomon metrics publishing
func Driver(registry metrics.Registry, opts ...option) trace.Driver {
	c := &config{
		registry:  registry,
		delimiter: "/",
	}
	for _, o := range opts {
		o(c)
	}
	if c.details == 0 {
		c.details =
			common.DriverClusterEvents |
				common.DriverConnEvents |
				common.DriverCredentialsEvents |
				common.DriverDiscoveryEvents
	}
	return common.Driver(c)
}
