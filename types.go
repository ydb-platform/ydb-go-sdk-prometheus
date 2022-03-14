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
		(5 * time.Millisecond).Seconds(),
		(10 * time.Millisecond).Seconds(),
		(25 * time.Millisecond).Seconds(),
		(50 * time.Millisecond).Seconds(),
		(100 * time.Millisecond).Seconds(),
		(250 * time.Millisecond).Seconds(),
		(500 * time.Millisecond).Seconds(),
		(1000 * time.Millisecond).Seconds(),
		(2500 * time.Millisecond).Seconds(),
		(5000 * time.Millisecond).Seconds(),
		(10000 * time.Millisecond).Seconds(),
		(25000 * time.Millisecond).Seconds(),
		(50000 * time.Millisecond).Seconds(),
		(100000 * time.Millisecond).Seconds(),
	}
)

type config struct {
	details   trace.Details
	separator string
	registry  prometheus.Registerer
	namespace string

	m          sync.Mutex
	counters   map[metricKey]*counterVec
	gauges     map[metricKey]*gaugeVec
	timers     map[metricKey]*timerVec
	histograms map[metricKey]*histogramVec
}

func (c *config) CounterVec(name string, labelNames ...string) registry.CounterVec {
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

func (c *config) WithSystem(subsystem string) registry.Config {
	return &config{
		separator:  c.separator,
		details:    c.details,
		registry:   c.registry,
		namespace:  c.join(c.namespace, subsystem),
		counters:   make(map[metricKey]*counterVec),
		gauges:     make(map[metricKey]*gaugeVec),
		timers:     make(map[metricKey]*timerVec),
		histograms: make(map[metricKey]*histogramVec),
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

func (c *counterVec) With(labels map[string]string) registry.Counter {
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

func (c *config) HistogramVec(name string, buckets []float64, labelNames ...string) registry.HistogramVec {
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
