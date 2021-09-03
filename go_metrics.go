package go_metrics

import (
	"github.com/YandexDatabase/ydb-go-monitoring-go-metrics/internal/common"
	metrics "github.com/rcrowley/go-metrics"
	ydb "github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
	"strings"
)

type gauge struct {
	g metrics.GaugeFloat64
}

func (g *gauge) With(tags map[string]string) common.Gauge {
	return g
}

func (g *gauge) Inc() {
	g.g.Update(g.g.Value() + 1)
}

func (g *gauge) Dec() {
	g.g.Update(g.g.Value() - 1)
}

func (g *gauge) Set(value float64) {
	g.g.Update(value)
}

type config struct {
	registry metrics.Registry
	prefix   []string
	tags     []string
}

func (c *config) Gauge(name string) common.Gauge {
	return &gauge{
		g: c.registry.GetOrRegister(
			strings.Join(append(c.prefix, name), "/")+"?"+strings.Join(c.tags, "&"),
			metrics.NewGaugeFloat64(),
		).(metrics.GaugeFloat64),
	}
}

func (c *config) WithTags(tags map[string]string) common.Config {
	keyValues := append(make([]string, 0, len(c.tags)+len(tags)), c.tags...)
	for k, v := range tags {
		keyValues = append(keyValues, k+"="+v)
	}
	return &config{
		registry: c.registry,
		prefix:   append(c.prefix),
		tags:     keyValues,
	}
}

func (c *config) WithPrefix(prefix string) common.Config {
	return &config{
		registry: c.registry,
		prefix:   append(c.prefix, prefix),
	}
}

// DriverTrace makes DriverTrace with solomon metrics publishing
func DriverTrace(registry metrics.Registry) ydb.DriverTrace {
	return common.DriverTrace(
		&config{
			registry: registry,
		},
	)
}

// ClientTrace makes table.ClientTrace with solomon metrics publishing
func ClientTrace(registry metrics.Registry) table.ClientTrace {
	return common.ClientTrace(
		&config{
			registry: registry,
		},
	)
}

// SessionPoolTrace makes table.SessionPoolTrace with solomon metrics publishing
func SessionPoolTrace(registry metrics.Registry) table.SessionPoolTrace {
	return common.SessionPoolTrace(
		&config{
			registry: registry,
		},
	)
}

// SessionPoolTrace makes table.SessionPoolTrace with solomon metrics publishing
func RetryTrace(registry metrics.Registry) table.RetryTrace {
	return common.RetryTrace(
		&config{
			registry: registry,
		},
	)
}
