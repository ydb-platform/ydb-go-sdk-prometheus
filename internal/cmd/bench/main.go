package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"

	metricscharts "github.com/aalpern/go-metrics-charts"
	"github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/resultset"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/ydb-go-sdk-metrics-go-metrics"
)

var (
	flagSet    = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	connection string
)

func init() {
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

type quet struct {
}

func (q quet) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func main() {
	err := flagSet.Parse(os.Args[1:])
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	var creds ydb.Option
	if token, has := os.LookupEnv("YDB_ACCESS_TOKEN_CREDENTIALS"); has {
		creds = ydb.WithCredentials(ydb.NewAuthTokenCredentials(token))
	}
	if v, has := os.LookupEnv("YDB_ANONYMOUS_CREDENTIALS"); has && v == "1" {
		creds = ydb.WithCredentials(ydb.NewAnonymousCredentials())
	}
	connectParams := ydb.MustConnectionString(connection)
	db, err := ydb.New(
		ctx,
		connectParams,
		ydb.WithDialTimeout(5*time.Second),
		creds,
		ydb.WithBalancingConfig(config.BalancerConfig{
			Algorithm:       config.BalancingAlgorithmP2C,
			PreferLocal:     false,
			OpTimeThreshold: time.Second,
		}),
		ydb.WithSessionPoolSizeLimit(100),
		ydb.WithSessionPoolIdleThreshold(time.Second*5),
		//ydb.WithTraceDriver(go_metrics.Driver(
		//	metrics.DefaultRegistry,
		//	go_metrics.WithDetails(common.DriverConnEvents|common.DriverDiscoveryEvents|common.DriverClusterEvents|common.DriverCredentialsEvents),
		//	go_metrics.WithDelimiter(" ➠ "),
		//)),
		ydb.WithTraceTable(go_metrics.Table(
			metrics.DefaultRegistry,
			go_metrics.WithDelimiter(" ➠ "),
		)),
		ydb.WithGrpcConnectionTTL(time.Second*5),
		//ydb.WithTableSessionPoolTrace(go_metrics.SessionPoolTrace(metrics.DefaultRegistry)),
	)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	log.SetOutput(&quet{})

	concurrency := 100

	wg := &sync.WaitGroup{}
	wg.Add(concurrency + 1)
	//go updateConnStats(wg, db)
	//go updatePoolStats(wg, db.Table())
	go httpServe(wg)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				_ = tick(ctx, db.Table())
				_ = selectSimple(ctx, db.Table(), connectParams.Database())
			}
		}()
	}
	wg.Wait()
}

func httpServe(wg *sync.WaitGroup) {
	defer wg.Done()
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}

//func updatePoolStats(wg *sync.WaitGroup, sp *table.SessionPool) {
//	defer wg.Done()
//	for {
//		time.Sleep(time.Second)
//		stats := sp.Stats()
//		metrics.DefaultRegistry.GetOrRegister(
//			"pool/waitQ",
//			metrics.NewGauge(),
//		).(metrics.Gauge).Update(int64(stats.WaitQ))
//		metrics.DefaultRegistry.GetOrRegister(
//			"pool/index",
//			metrics.NewGauge(),
//		).(metrics.Gauge).Update(int64(stats.Index))
//		metrics.DefaultRegistry.GetOrRegister(
//			"pool/idle",
//			metrics.NewGauge(),
//		).(metrics.Gauge).Update(int64(stats.Idle))
//		metrics.DefaultRegistry.GetOrRegister(
//			"pool/minSize",
//			metrics.NewGauge(),
//		).(metrics.Gauge).Update(int64(stats.MinSize))
//		metrics.DefaultRegistry.GetOrRegister(
//			"pool/maxSize",
//			metrics.NewGauge(),
//		).(metrics.Gauge).Update(int64(stats.MaxSize))
//		metrics.DefaultRegistry.GetOrRegister(
//			"pool/createInProgress",
//			metrics.NewGauge(),
//		).(metrics.Gauge).Update(int64(stats.CreateInProgress))
//	}
//}
//
//func updateConnStats(wg *sync.WaitGroup, cluster ydb.Cluster) {
//	defer wg.Done()
//	for {
//		time.Sleep(time.Second)
//
//		actual := make(map[ydb.Endpoint]ydb.ConnState)
//
//		cluster.Stats(func(endpoint ydb.Endpoint, stats ydb.ConnStats) {
//			actual[endpoint] = stats.State
//			metrics.DefaultRegistry.GetOrRegister(
//				fmt.Sprintf("cluster/%s:%v/status", endpoint.Addr, endpoint.Port),
//				metrics.NewGauge(),
//			).(metrics.Gauge).Update(int64(stats.State))
//			metrics.DefaultRegistry.GetOrRegister(
//				fmt.Sprintf("cluster/%s:%v/OpPerMinute", endpoint.Addr, endpoint.Port),
//				metrics.NewGauge(),
//			).(metrics.Gauge).Update(
//				int64(stats.OpPerMinute),
//			)
//			metrics.DefaultRegistry.GetOrRegister(
//				fmt.Sprintf("cluster/%s:%v/AvgOpTime", endpoint.Addr, endpoint.Port),
//				metrics.NewGaugeFloat64(),
//			).(metrics.GaugeFloat64).Update(
//				float64(stats.AvgOpTime) / float64(time.Millisecond),
//			)
//			metrics.DefaultRegistry.GetOrRegister(
//				fmt.Sprintf("cluster/%s:%v/ErrPerMinute", endpoint.Addr, endpoint.Port),
//				metrics.NewGauge(),
//			).(metrics.Gauge).Update(
//				int64(stats.ErrPerMinute),
//			)
//			metrics.DefaultRegistry.GetOrRegister(
//				fmt.Sprintf("cluster/%s:%v/OpFailed", endpoint.Addr, endpoint.Port),
//				metrics.NewGauge(),
//			).(metrics.Gauge).Update(
//				int64(stats.OpFailed),
//			)
//		})
//
//		endpointsMtx.Lock()
//		for endpoint := range endpoints {
//			if _, ok := actual[endpoint]; !ok {
//				metrics.DefaultRegistry.GetOrRegister(
//					fmt.Sprintf("cluster/%s:%v/OpPerMinute", endpoint.Addr, endpoint.Port),
//					metrics.NewGauge(),
//				).(metrics.Gauge).Update(0)
//				metrics.DefaultRegistry.GetOrRegister(
//					fmt.Sprintf("cluster/%s:%v/AvgOpTime", endpoint.Addr, endpoint.Port),
//					metrics.NewGaugeFloat64(),
//				).(metrics.GaugeFloat64).Update(0)
//				metrics.DefaultRegistry.GetOrRegister(
//					fmt.Sprintf("cluster/%s:%v/ErrPerMinute", endpoint.Addr, endpoint.Port),
//					metrics.NewGauge(),
//				).(metrics.Gauge).Update(0)
//				metrics.DefaultRegistry.GetOrRegister(
//					fmt.Sprintf("cluster/%s:%v/OpFailed", endpoint.Addr, endpoint.Port),
//					metrics.NewGauge(),
//				).(metrics.Gauge).Update(0)
//				endpoints[endpoint] = ydb.ConnStateUnknown
//			}
//		}
//		for e, state := range actual {
//			endpoints[e] = state
//		}
//		for s := ydb.ConnOffline; s <= ydb.ConnOnline; s++ {
//			metrics.DefaultRegistry.GetOrRegister(
//				fmt.Sprintf("endpoints/%s", s.String()),
//				metrics.NewGauge(),
//			).(metrics.Gauge).Update(func() (count int64) {
//				for _, state := range endpoints {
//					if state == s {
//						count++
//					}
//				}
//				return count
//			}())
//		}
//		endpointsMtx.Unlock()
//	}
//}

func selectSimple(ctx context.Context, c table.Client, prefix string) error {
	query := fmt.Sprintf(`
			PRAGMA TablePathPrefix("%s");
			DECLARE $seriesID AS Uint64;
			$format = DateTime::Format("%%Y-%%m-%%d");
			SELECT
				series_id,
				title,
				$format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date
			FROM
				series
			WHERE
				series_id = $seriesID;
		`, prefix)
	readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly(table.WithInconsistentReads())), table.CommitTx())
	var res resultset.Result
	err := c.RetryIdempotent(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return
			}
			_, res, err = stmt.Execute(ctx, readTx,
				table.NewQueryParameters(
					table.ValueParam("$seriesID", types.Uint64Value(1)),
				),
				options.WithQueryCachePolicy(
					options.WithQueryCachePolicyKeepInCache(),
				),
				options.WithCollectStatsModeBasic(),
			)
			return
		},
	)
	if err != nil {
		return err
	}

	var (
		id    *uint64
		title *string
		date  *[]byte
	)

	log.Printf("> select_simple_transaction:\n")
	for res.NextResultSet(ctx, "series_id", "title", "release_date") {
		for res.NextRow() {
			err = res.Scan(&id, &title, &date)
			if err != nil {
				return err
			}
			log.Printf(
				"  > %d %s %s\n",
				*id, *title, *date,
			)
		}
	}
	return res.Err()
}

func tick(ctx context.Context, tbl table.Client) (err error) {
	//now := time.Now()
	query := `SELECT 1, "1", 1+1;`
	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
	defer cancel()
	txc := table.TxControl(table.BeginTx(table.WithOnlineReadOnly(table.WithInconsistentReads())), table.CommitTx())
	err = tbl.RetryIdempotent(
		ctx,
		func(ctx context.Context, session table.Session) error {
			_, result, err := session.Execute(
				ctx,
				txc,
				query,
				table.NewQueryParameters(),
				options.WithQueryCachePolicy(
					options.WithQueryCachePolicyKeepInCache(),
				),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = result.Close()
			}()
			return nil
		},
	)
	//if err == nil {
	//	timeout := metrics.DefaultRegistry.GetOrRegister(
	//		"tick/timeout",
	//		metrics.NewGaugeFloat64(),
	//	).(metrics.GaugeFloat64)
	//	timeout.Update(timeout.Value()*0.9 + 0.1*float64(time.Since(now).Milliseconds()))
	//	metrics.DefaultRegistry.GetOrRegister(
	//		"tick/count",
	//		metrics.NewCounter(),
	//	).(metrics.Counter).Inc(1)
	//} else {
	//	timeout := metrics.DefaultRegistry.GetOrRegister(
	//		"tick/errors/timeout",
	//		metrics.NewGaugeFloat64(),
	//	).(metrics.GaugeFloat64)
	//	timeout.Update(timeout.Value()*0.9 + 0.1*float64(time.Since(now).Milliseconds()))
	//	metrics.DefaultRegistry.GetOrRegister(
	//		"tick/errors",
	//		metrics.NewCounter(),
	//	).(metrics.Counter).Inc(1)
	//}
	return err
}
