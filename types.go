package metrics

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-sdk-metrics/registry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	defaultNamespace = "ydb_go_sdk"
	defaultSeparator = "_"
)

var (
	defaultTimerBuckets = []float64{
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
	timers     map[timerOpts]*timerVec
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

func (c *config) WithSystem(subsystem string) registry.Config {
	return &config{
		separator:  c.separator,
		details:    c.details,
		registry:   c.registry,
		namespace:  c.join(c.namespace, subsystem),
		gauges:     make(map[gaugeOpts]*gaugeVec),
		timers:     make(map[timerOpts]*timerVec),
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

type timerOpts struct {
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

func newTimerOpts(opts prometheus.HistogramOpts) timerOpts {
	return timerOpts{
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

type timerVec struct {
	t *prometheus.HistogramVec
}

type timer struct {
	o prometheus.Observer
}

type histogram struct {
	o prometheus.Observer
}

func (h *timer) Record(d time.Duration) {
	h.o.Observe(d.Seconds())
}

func (h *histogram) Record(v float64) {
	h.o.Observe(v)
}

func (h *timerVec) With(labels map[string]string) registry.Timer {
	observer, err := h.t.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return &timer{o: observer}
}

func (h *histogramVec) With(labels map[string]string) registry.Histogram {
	observer, err := h.h.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return &histogram{o: observer}
}

func (g *gaugeVec) With(labels map[string]string) registry.Gauge {
	gauge, err := g.g.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return gauge
}

func (c *config) GaugeVec(name string, labelNames ...string) registry.GaugeVec {
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

func (c *config) TimerVec(name string, labelNames ...string) registry.TimerVec {
	opts := prometheus.HistogramOpts{
		Namespace: c.namespace,
		Name:      name,
		Buckets:   defaultTimerBuckets,
	}
	timersOpts := newTimerOpts(opts)
	c.m.Lock()
	defer c.m.Unlock()
	if t, ok := c.timers[timersOpts]; ok {
		return t
	}
	t := &timerVec{t: prometheus.NewHistogramVec(opts, labelNames)}
	if err := c.registry.Register(t.t); err != nil {
		panic(err)
	}
	c.timers[timersOpts] = t
	return t
}

func (c *config) HistogramVec(name string, labelNames ...string) registry.HistogramVec {
	opts := prometheus.HistogramOpts{
		Namespace: c.namespace,
		Name:      name,
		Buckets:   defaultTimerBuckets,
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
