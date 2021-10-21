package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	sensors "github.com/ydb-platform/ydb-go-sdk-sensors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Table makes table.ClientTrace with solomon metrics publishing
func Table(registry prometheus.Registerer, opts ...option) trace.Table {
	c := &config{
		registry:  registry,
		namespace: "ydb_go_sdk",
		separator: "_",
	}
	for _, o := range opts {
		o(c)
	}

	if c.details == 0 {
		c.details = ^sensors.Details(0)
	}
	return sensors.Table(c)
}
