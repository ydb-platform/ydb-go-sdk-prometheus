package metrics

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-sdk-metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"strings"
	"sync"
	"time"
)

const (
	defaultNamespace = "ydb_go_sdk"
	defaultSeparator = "_"
)

var (
	defaultBuckets = []float64{
		float64(5*time.Millisecond) / float64(time.Second),
		float64(10*time.Millisecond) / float64(time.Second),
		float64(25*time.Millisecond) / float64(time.Second),
		float64(50*time.Millisecond) / float64(time.Second),
		float64(100*time.Millisecond) / float64(time.Second),
		float64(250*time.Millisecond) / float64(time.Second),
		float64(500*time.Millisecond) / float64(time.Second),
		float64(1000*time.Millisecond) / float64(time.Second),
		float64(2500*time.Millisecond) / float64(time.Second),
		float64(5000*time.Millisecond) / float64(time.Second),
		float64(10000*time.Millisecond) / float64(time.Second),
		float64(25000*time.Millisecond) / float64(time.Second),
		float64(50000*time.Millisecond) / float64(time.Second),
		float64(100000*time.Millisecond) / float64(time.Second),
	}
)

type config struct {
	details   trace.Details
	separator string
	registry  prometheus.Registerer
	namespace string

	m          sync.Mutex
	gauges     map[gaugeOpts]*gaugeVec
	histograms map[histogramsOpts]*histogramVec
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
		separator:  c.separator,
		details:    c.details,
		registry:   c.registry,
		namespace:  c.join(c.namespace, subsystem),
		gauges:     make(map[gaugeOpts]*gaugeVec),
		histograms: make(map[histogramsOpts]*histogramVec),
	}
}

type gaugeOpts struct {
	Namespace string
	Subsystem string
	Name      string
}

func newGaugeOpts(opts prometheus.GaugeOpts) gaugeOpts {
	return gaugeOpts{
		Namespace: opts.Namespace,
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
	}
}

type histogramsOpts struct {
	Namespace string
	Subsystem string
	Name      string
	Buckets   string
}

func newHistogramOpts(opts prometheus.HistogramOpts) histogramsOpts {
	return histogramsOpts{
		Namespace: opts.Namespace,
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
		Buckets:   fmt.Sprintf("%v", opts.Buckets),
	}
}

type gaugeVec struct {
	g *prometheus.GaugeVec
}

type histogramVec struct {
	h *prometheus.HistogramVec
}

type timer struct {
	o prometheus.Observer
}

func (h *timer) Record(d time.Duration) {
	h.o.Observe(d.Seconds())
}

func (h *histogramVec) With(labels map[string]string) metrics.Timer {
	observer, err := h.h.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return &timer{o: observer}
}

func (g *gaugeVec) With(labels map[string]string) metrics.Gauge {
	gauge, err := g.g.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return gauge
}

func (c *config) GaugeVec(name string, labelNames ...string) metrics.GaugeVec {
	opts := prometheus.GaugeOpts{
		Namespace: c.namespace,
		Name:      name,
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

func (c *config) TimerVec(name string, labelNames ...string) metrics.TimerVec {
	opts := prometheus.HistogramOpts{
		Namespace: c.namespace,
		Name:      name,
		Buckets:   defaultBuckets,
	}
	histogramsOpts := newHistogramOpts(opts)
	c.m.Lock()
	defer c.m.Unlock()
	if h, ok := c.histograms[histogramsOpts]; ok {
		return h
	}
	h := &histogramVec{h: prometheus.NewHistogramVec(opts, labelNames)}
	if err := c.registry.Register(h.h); err != nil {
		panic(err)
	}
	c.histograms[histogramsOpts] = h
	return h
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
		c.details |= details
	}
}

func WithSeparator(separator string) option {
	return func(c *config) {
		c.separator = separator
	}
}
