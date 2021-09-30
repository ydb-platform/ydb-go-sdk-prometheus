package go_metrics

import (
	metrics "github.com/rcrowley/go-metrics"
	"github.com/ydb-platform/ydb-go-sdk-metrics-go-metrics/internal/common"
)

type gauge struct {
	g metrics.GaugeFloat64
}

func (g *gauge) Value() float64 {
	return g.g.Value()
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
	details   common.Details
	names     map[common.GaugeType]string
	delimiter string
	prefix    string
	registry  metrics.Registry
}

func (c *config) Details() common.Details {
	return c.details
}

func (c *config) Gauge(name string) common.Gauge {
	return &gauge{
		g: c.registry.GetOrRegister(name, metrics.NewGaugeFloat64()).(metrics.GaugeFloat64),
	}
}

func (c *config) Name(gaugeType common.GaugeType) *string {
	if n, ok := c.names[gaugeType]; ok {
		return &n
	}
	return nil
}

func (c *config) Delimiter() *string {
	if c.delimiter == "" {
		return nil
	}
	return &c.delimiter
}

func (c *config) Prefix() *string {
	if c.prefix == "" {
		return nil
	}
	return &c.prefix
}

func (c *config) Join(parts ...common.GaugeName) *string {
	return nil
}

func (c *config) ErrName(err error) *string {
	return nil
}

type option func(*config)

func WithNames(names map[common.GaugeType]string) option {
	return func(c *config) {
		for k, v := range names {
			c.names[k] = v
		}
	}
}

func WithPrefix(prefix string) option {
	return func(c *config) {
		c.prefix = prefix
	}
}

func WithDelimiter(delimiter string) option {
	return func(c *config) {
		c.delimiter = delimiter
	}
}

func WithDetails(details common.Details) option {
	return func(c *config) {
		c.details = details
	}
}

//
//// SessionPoolTrace makes table.SessionPoolTrace with solomon metrics publishing
//func RetryTrace(registry metrics.Registry) ydb.RetryTrace {
//	return common.RetryTrace(
//		&config{
//			registry: registry,
//		},
//	)
//}
