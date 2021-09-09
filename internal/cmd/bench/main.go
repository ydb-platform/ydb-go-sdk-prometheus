package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"

	metricscharts "github.com/aalpern/go-metrics-charts"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"

	"github.com/ydb-platform/ydb-go-monitoring-go-metrics"
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/connect"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"

	"github.com/rs/zerolog"
)

var (
	endpoints    = make(map[ydb.Endpoint]ydb.ConnState)
	endpointsMtx sync.Mutex
	logger       = zerolog.New(os.Stdout)
	flagSet      = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	connection   string
)

func init() {
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	if e, ok := os.LookupEnv("LOG_LEVEL"); ok {
		if l, err := zerolog.ParseLevel(e); err == nil {
			zerolog.SetGlobalLevel(l)
		}
	}
	exp.Exp(metrics.DefaultRegistry)
	metricscharts.Register()
	flagSet.Usage = func() {
		out := flagSet.Output()
		_, _ = fmt.Fprintf(out, "Usage:\n%s command [options]\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "\nOptions:\n")
		flagSet.PrintDefaults()
	}
	flagSet.StringVar(&connection,
		"ydb", "",
		"YDB connection string",
	)
}

func main() {
	err := flagSet.Parse(os.Args[1:])
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	ctx = ydb.WithDriverTrace(ctx, go_metrics.DriverTrace(metrics.DefaultRegistry))
	ctx = table.WithClientTrace(ctx, go_metrics.ClientTrace(metrics.DefaultRegistry))
	ctx = table.WithSessionPoolTrace(ctx, go_metrics.SessionPoolTrace(metrics.DefaultRegistry))
	ctx = table.WithRetryTrace(ctx, go_metrics.RetryTrace(metrics.DefaultRegistry))
	cluster, err := connect.New(
		ctx,
		connect.MustConnectionString(connection),
	)
	if err != nil {
		panic(err)
	}
	defer cluster.Close()

	concurrency := 50

	logger.Info().Caller().Int("concurrency", concurrency).Msg("")

	wg := &sync.WaitGroup{}
	wg.Add(concurrency + 3)
	go updateConnStats(wg, cluster.Driver())
	go updatePoolStats(wg, cluster.Table().Pool())
	go httpServe(wg)
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			for {
				if err := tick(ctx, cluster.Table().Pool()); err != nil {
					if tr, ok := err.(*ydb.TransportError); ok && tr.Reason == ydb.TransportErrorResourceExhausted {
						panic("error returned to user (" + err.Error() + ")")
					}
				}
			}
		}(i)
	}
	wg.Wait()

	logger.Debug().Caller().Msg("")
}

func httpServe(wg *sync.WaitGroup) {
	defer wg.Done()
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}

func updatePoolStats(wg *sync.WaitGroup, sp *table.SessionPool) {
	defer wg.Done()
	for {
		time.Sleep(time.Second)
		stats := sp.Stats()
		metrics.DefaultRegistry.GetOrRegister(
			"pool/waitQ",
			metrics.NewGauge(),
		).(metrics.Gauge).Update(int64(stats.WaitQ))
		metrics.DefaultRegistry.GetOrRegister(
			"pool/index",
			metrics.NewGauge(),
		).(metrics.Gauge).Update(int64(stats.Index))
		metrics.DefaultRegistry.GetOrRegister(
			"pool/idle",
			metrics.NewGauge(),
		).(metrics.Gauge).Update(int64(stats.Idle))
		metrics.DefaultRegistry.GetOrRegister(
			"pool/ready",
			metrics.NewGauge(),
		).(metrics.Gauge).Update(int64(stats.Ready))
		metrics.DefaultRegistry.GetOrRegister(
			"pool/minSize",
			metrics.NewGauge(),
		).(metrics.Gauge).Update(int64(stats.MinSize))
		metrics.DefaultRegistry.GetOrRegister(
			"pool/maxSize",
			metrics.NewGauge(),
		).(metrics.Gauge).Update(int64(stats.MaxSize))
		metrics.DefaultRegistry.GetOrRegister(
			"pool/createInProgress",
			metrics.NewGauge(),
		).(metrics.Gauge).Update(int64(stats.CreateInProgress))
		metrics.DefaultRegistry.GetOrRegister(
			"pool/busyCheck",
			metrics.NewGauge(),
		).(metrics.Gauge).Update(int64(stats.BusyCheck))
	}
}

func updateConnStats(wg *sync.WaitGroup, d ydb.Driver) {
	defer wg.Done()
	for {
		time.Sleep(time.Second)

		actual := make(map[ydb.Endpoint]ydb.ConnState)

		ydb.ReadConnStats(d, func(endpoint ydb.Endpoint, stats ydb.ConnStats) {
			actual[endpoint] = stats.State
			metrics.DefaultRegistry.GetOrRegister(
				fmt.Sprintf("%s:%d/status", endpoint.Addr, endpoint.Port),
				metrics.NewGauge(),
			).(metrics.Gauge).Update(int64(stats.State))
			metrics.DefaultRegistry.GetOrRegister(
				fmt.Sprintf("%s:%d/OpPerMinute", endpoint.Addr, endpoint.Port),
				metrics.NewGauge(),
			).(metrics.Gauge).Update(
				int64(stats.OpPerMinute),
			)
			metrics.DefaultRegistry.GetOrRegister(
				fmt.Sprintf("%s:%d/AvgOpTime", endpoint.Addr, endpoint.Port),
				metrics.NewGaugeFloat64(),
			).(metrics.GaugeFloat64).Update(
				float64(stats.AvgOpTime) / float64(time.Millisecond),
			)
			metrics.DefaultRegistry.GetOrRegister(
				fmt.Sprintf("%s:%d/ErrPerMinute", endpoint.Addr, endpoint.Port),
				metrics.NewGauge(),
			).(metrics.Gauge).Update(
				int64(stats.ErrPerMinute),
			)
			metrics.DefaultRegistry.GetOrRegister(
				fmt.Sprintf("%s:%d/OpFailed", endpoint.Addr, endpoint.Port),
				metrics.NewGauge(),
			).(metrics.Gauge).Update(
				int64(stats.OpFailed),
			)
		})

		endpointsMtx.Lock()
		for endpoint := range endpoints {
			if _, ok := actual[endpoint]; !ok {
				metrics.DefaultRegistry.GetOrRegister(
					fmt.Sprintf("%s:%d/OpPerMinute", endpoint.Addr, endpoint.Port),
					metrics.NewGauge(),
				).(metrics.Gauge).Update(0)
				metrics.DefaultRegistry.GetOrRegister(
					fmt.Sprintf("%s:%d/AvgOpTime", endpoint.Addr, endpoint.Port),
					metrics.NewGaugeFloat64(),
				).(metrics.GaugeFloat64).Update(0)
				metrics.DefaultRegistry.GetOrRegister(
					fmt.Sprintf("%s:%d/ErrPerMinute", endpoint.Addr, endpoint.Port),
					metrics.NewGauge(),
				).(metrics.Gauge).Update(0)
				metrics.DefaultRegistry.GetOrRegister(
					fmt.Sprintf("%s:%d/OpFailed", endpoint.Addr, endpoint.Port),
					metrics.NewGauge(),
				).(metrics.Gauge).Update(0)
				endpoints[endpoint] = ydb.ConnStateUnknown
			}
		}
		for e, state := range actual {
			endpoints[e] = state
		}
		for s := ydb.ConnOffline; s <= ydb.ConnOnline; s++ {
			metrics.DefaultRegistry.GetOrRegister(
				fmt.Sprintf("endpoints/%s", s.String()),
				metrics.NewGauge(),
			).(metrics.Gauge).Update(func() (count int64) {
				for _, state := range endpoints {
					if state == s {
						count++
					}
				}
				return count
			}())
		}
		endpointsMtx.Unlock()
	}
}

func processError(prefix string, err error) {
	go func() {
		metrics.DefaultRegistry.GetOrRegister(
			prefix+"/all",
			metrics.NewCounter(),
		).(metrics.Counter).Inc(1)
	}()
	go func() {
		var op *ydb.OpError
		if errors.As(err, &op) {
			metrics.DefaultRegistry.GetOrRegister(
				prefix+"/operation",
				metrics.NewCounter(),
			).(metrics.Counter).Inc(1)
			metrics.DefaultRegistry.GetOrRegister(
				prefix+"/operation/"+op.Reason.String(),
				metrics.NewCounter(),
			).(metrics.Counter).Inc(1)
		}
	}()
	go func() {
		var te *ydb.TransportError
		if errors.As(err, &te) {
			metrics.DefaultRegistry.GetOrRegister(
				prefix+"/transport",
				metrics.NewCounter(),
			).(metrics.Counter).Inc(1)
			metrics.DefaultRegistry.GetOrRegister(
				prefix+"/transport/"+te.Reason.String(),
				metrics.NewCounter(),
			).(metrics.Counter).Inc(1)
		}
	}()
	go func() {
		if errors.Is(err, context.DeadlineExceeded) {
			metrics.DefaultRegistry.GetOrRegister(
				prefix+"/context/DeadlineExceeded",
				metrics.NewCounter(),
			).(metrics.Counter).Inc(1)
		}
	}()
	go func() {
		if errors.Is(err, table.ErrSessionPoolOverflow) {
			metrics.DefaultRegistry.GetOrRegister(
				prefix+"/ErrSessionPoolOverflow",
				metrics.NewCounter(),
			).(metrics.Counter).Inc(1)
		}
	}()
}

func tick(ctx context.Context, sp *table.SessionPool) (err error) {
	now := time.Now()
	query := `SELECT 1, "1", 1+1;`
	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*50)
	defer cancel()
	err = table.Retry(ctx, sp, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		txc := table.TxControl(table.BeginTx(table.WithOnlineReadOnly(table.WithInconsistentReads())), table.CommitTx())
		_, result, err := session.Execute(ctx, txc, query, nil, table.WithQueryCachePolicy(table.WithQueryCachePolicyKeepInCache()))
		if err != nil {
			processError("errors/session", err)
			return err
		}
		defer func() {
			_ = result.Close()
		}()
		return nil
	}))
	if err == nil {
		timeout := metrics.DefaultRegistry.GetOrRegister(
			"tick/timeout",
			metrics.NewGaugeFloat64(),
		).(metrics.GaugeFloat64)
		timeout.Update(timeout.Value()*0.9 + 0.1*float64(time.Since(now).Milliseconds()))
		metrics.DefaultRegistry.GetOrRegister(
			"tick/count",
			metrics.NewCounter(),
		).(metrics.Counter).Inc(1)
	} else {
		processError("tick/errors", err)
		timeout := metrics.DefaultRegistry.GetOrRegister(
			"tick/errors/timeout",
			metrics.NewGaugeFloat64(),
		).(metrics.GaugeFloat64)
		timeout.Update(timeout.Value()*0.9 + 0.1*float64(time.Since(now).Milliseconds()))
		metrics.DefaultRegistry.GetOrRegister(
			"tick/errors",
			metrics.NewCounter(),
		).(metrics.Counter).Inc(1)
	}
	return err
}
