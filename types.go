package sensors

import (
	"github.com/prometheus/client_golang/prometheus"
	sensors "github.com/ydb-platform/ydb-go-sdk-sensors"
	"strings"
	"sync"
)

type config struct {
	details   sensors.Details
	registry  prometheus.Registerer
	namespace string

	m      sync.Mutex
	gauges map[gaugeOpts]*gaugeVec
}

func join(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return ""
	}
	return strings.Join([]string{a, b}, "_")
}

func (c *config) WithSystem(subsystem string) sensors.Config {
	return &config{
		details:   c.details,
		registry:  c.registry,
		namespace: join(c.namespace, subsystem),
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

func (g *gaugeVec) With(labels ...sensors.Label) sensors.Gauge {
	gauge, err := g.g.GetMetricWith(func() prometheus.Labels {
		kv := make(prometheus.Labels, len(labels))
		for _, label := range labels {
			kv[string(label.Tag)] = string(label.Value)
		}
		return kv
	}())
	if err != nil {
		panic(err)
	}
	return gauge
}

func (c *config) GaugeVec(name string, description string, labelNames ...string) sensors.GaugeVec {
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
	c.registry.Register(g.g)
	c.gauges[gaugeOpts] = g
	return g
}

func (c *config) Details() sensors.Details {
	return c.details
}

type option func(*config)

func WithNamespace(namespace string) option {
	return func(c *config) {
		c.namespace = namespace
	}
}

func WithDetails(details sensors.Details) option {
	return func(c *config) {
		c.details = details
	}
}
