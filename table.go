package metrics_local

import (
	"github.com/rcrowley/go-metrics"
	common "github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes table.ClientTrace with solomon metrics publishing
func Table(registry metrics.Registry, opts ...option) trace.Table {
	c := &config{
		registry:  registry,
		delimiter: "/",
	}
	for _, o := range opts {
		o(c)
	}

	if c.details == 0 {
		c.details =
			common.TableSessionEvents |
				common.TableQueryEvents |
				common.TableStreamEvents |
				common.TableTransactionEvents |
				common.TablePoolEvents
	}
	return common.Table(c)
}
