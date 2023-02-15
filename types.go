package metrics

import (
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	defaultNamespace = "ydb_go_sdk"
	defaultSeparator = "_"
)

var (
	defaultTimerBuckets = prometheus.ExponentialBuckets(time.Millisecond.Seconds(), 1.25, 100)
)

var _ metrics.Config = (*config)(nil)

type config struct {
	details      trace.Details
	separator    string
	registry     prometheus.Registerer
	namespace    string
	timerBuckets []float64

	m          sync.Mutex
	counters   map[metricKey]*counterVec
	gauges     map[metricKey]*gaugeVec
	timers     map[metricKey]*timerVec
	histograms map[metricKey]*histogramVec
}

func Config(registry prometheus.Registerer, opts ...option) *config {
	c := &config{
		registry:     registry,
		details:      trace.DetailsAll,
		namespace:    defaultNamespace,
		separator:    defaultSeparator,
		timerBuckets: defaultTimerBuckets,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *config) CounterVec(name string, labelNames ...string) metrics.CounterVec {
	opts := prometheus.CounterOpts{
		Namespace: c.namespace,
		Name:      name,
	}
	counterOpts := newCounterOpts(opts)
	c.m.Lock()
	defer c.m.Unlock()
	if cnt, ok := c.counters[counterOpts]; ok {
		return cnt
	}
	cnt := &counterVec{c: prometheus.NewCounterVec(opts, labelNames)}
	if err := c.registry.Register(cnt.c); err != nil {
		panic(err)
	}
	c.counters[counterOpts] = cnt
	return cnt
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
		separator:    c.separator,
		details:      c.details,
		registry:     c.registry,
		timerBuckets: c.timerBuckets,
		namespace:    c.join(c.namespace, subsystem),
		counters:     make(map[metricKey]*counterVec),
		gauges:       make(map[metricKey]*gaugeVec),
		timers:       make(map[metricKey]*timerVec),
		histograms:   make(map[metricKey]*histogramVec),
	}
}

type metricKey struct {
	Namespace string
	Subsystem string
	Name      string
	Buckets   string
}

func newCounterOpts(opts prometheus.CounterOpts) metricKey {
	return metricKey{
		Namespace: opts.Namespace,
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
	}
}

func newGaugeOpts(opts prometheus.GaugeOpts) metricKey {
	return metricKey{
		Namespace: opts.Namespace,
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
	}
}

func newHistogramOpts(opts prometheus.HistogramOpts) metricKey {
	return metricKey{
		Namespace: opts.Namespace,
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
		Buckets:   fmt.Sprintf("%v", opts.Buckets),
	}
}

func newTimerOpts(opts prometheus.HistogramOpts) metricKey {
	return metricKey{
		Namespace: opts.Namespace,
		Subsystem: opts.Subsystem,
		Name:      opts.Name,
		Buckets:   fmt.Sprintf("%v", opts.Buckets),
	}
}

type counterVec struct {
	c *prometheus.CounterVec
}

func (c *counterVec) With(labels map[string]string) metrics.Counter {
	cnt, err := c.c.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return cnt
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

func (h *timerVec) With(labels map[string]string) metrics.Timer {
	observer, err := h.t.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return &timer{o: observer}
}

func (h *histogramVec) With(labels map[string]string) metrics.Histogram {
	observer, err := h.h.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return &histogram{o: observer}
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
		Buckets:   c.timerBuckets,
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

func (c *config) HistogramVec(name string, buckets []float64, labelNames ...string) metrics.HistogramVec {
	opts := prometheus.HistogramOpts{
		Namespace: c.namespace,
		Name:      name,
		Buckets:   buckets,
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

func WithTimerBuckets(timerBuckets []float64) option {
	return func(c *config) {
		c.timerBuckets = timerBuckets
	}
}
