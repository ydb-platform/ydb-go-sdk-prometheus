package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"strings"
	"sync"
)

const (
	defaultNamespace = "ydb_go_sdk"
	defaultSeparator = "_"
)

type config struct {
	details   trace.Details
	separator string
	registry  prometheus.Registerer
	namespace string

	m      sync.Mutex
	gauges map[gaugeOpts]*gaugeVec
}

func (c *config) join(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return ""
	}
	return strings.Join([]string{a, b}, c.separator)
}

func (c *config) WithSystem(subsystem string) metrics.Config {
	return &config{
		separator: c.separator,
		details:   c.details,
		registry:  c.registry,
		namespace: c.join(c.namespace, subsystem),
		gauges:    make(map[gaugeOpts]*gaugeVec),
	}
}

type gaugeOpts struct {
	Namespace   string
	Subsystem   string
	Name        string
	Description string
}

func newGaugeOpts(opts prometheus.GaugeOpts) gaugeOpts {
	return gaugeOpts{
		Namespace:   opts.Namespace,
		Subsystem:   opts.Subsystem,
		Name:        opts.Name,
		Description: opts.Help,
	}
}

type gaugeVec struct {
	g *prometheus.GaugeVec
}

func (g *gaugeVec) With(labels map[string]string) metrics.Gauge {
	gauge, err := g.g.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return gauge
}

func (c *config) GaugeVec(name string, description string, labelNames ...string) metrics.GaugeVec {
	opts := prometheus.GaugeOpts{
		Namespace: c.namespace,
		Name:      name,
		Help:      description,
	}
	gaugeOpts := newGaugeOpts(opts)
	c.m.Lock()
	defer c.m.Unlock()
	if g, ok := c.gauges[gaugeOpts]; ok {
		return g
	}
	g := &gaugeVec{g: prometheus.NewGaugeVec(opts, labelNames)}
	if err := c.registry.Register(g.g); err != nil {
		panic(err)
	}
	c.gauges[gaugeOpts] = g
	return g
}

func (c *config) Details() trace.Details {
	return c.details
}

type option func(*config)

func WithNamespace(namespace string) option {
	return func(c *config) {
		c.namespace = namespace
	}
}

func WithDetails(details trace.Details) option {
	return func(c *config) {
		c.details = details
	}
}

func WithSeparator(separator string) option {
	return func(c *config) {
		c.separator = separator
	}
}
