package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	metrics "github.com/ydb-platform/ydb-go-sdk-prometheus"
)

var (
	flagSet    = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	connection string
	port       int
)

func init() {
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
	flagSet.IntVar(&port,
		"port", 8080,
		"prometheus agent port",
	)
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 500
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
		creds = ydb.WithAccessTokenCredentials(token)
	}
	if v, has := os.LookupEnv("YDB_ANONYMOUS_CREDENTIALS"); has && v == "1" {
		creds = ydb.WithAnonymousCredentials()
	}
	registry := prometheus.NewRegistry()
	db, err := ydb.New(
		ctx,
		ydb.WithConnectionString(connection),
		ydb.WithDialTimeout(5*time.Second),
		ydb.WithBalancingConfig(config.BalancerConfig{
			Algorithm:   config.BalancingAlgorithmRandomChoice,
			PreferLocal: false,
		}),
		//ydb.WithDiscoveryInterval(5 * time.Second),
		ydb.WithCertificatesFromFile("~/ydb_certs/ca.pem"),
		creds,
		ydb.WithSessionPoolSizeLimit(300),
		ydb.WithSessionPoolIdleThreshold(time.Second*5),
		ydb.WithTraceDriver(metrics.Driver(
			registry,
			metrics.WithSeparator("_"),
		)),
		ydb.WithTraceTable(metrics.Table(
			registry,
			metrics.WithSeparator("_"),
		)),
		ydb.WithGrpcConnectionTTL(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	log.SetOutput(&quet{})

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go httpServe(wg, port, registry)

	if concurrency, err := strconv.Atoi(os.Getenv("YDB_PREPARE_BENCH_DATA")); err == nil && concurrency > 0 {
		_ = upsertData(ctx, db.Table(), db.Name(), "series", registry, concurrency)
	}

	concurrency := 300
	wg.Add(concurrency)

	inFlight := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "app",
		Name:      "in_flight",
	}, []string{}).With(prometheus.Labels{})
	_ = registry.Register(inFlight)
	rows := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "app",
		Name:      "rows_per_request",
	}, []string{"success"})
	_ = registry.Register(rows)
	latency := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "app",
		Name:      "latency_per_request",
	}, []string{"success"})
	_ = registry.Register(latency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
				inFlight.Add(1)
				start := time.Now()
				count, err := scanSelect(
					ctx,
					db.Table(),
					db.Name(),
					rand.Int63n(2500),
				)
				inFlight.Add(-1)
				success := map[string]string{
					"success": func() string {
						if err == nil {
							return "true"
						}
						return "false"
					}(),
				}
				latency.With(success).Set(float64(time.Since(start)))
				rows.With(success).Set(float64(count))
			}
		}()
	}
	wg.Wait()
}

func httpServe(wg *sync.WaitGroup, port int, registry *prometheus.Registry) {
	defer wg.Done()
	time.Sleep(time.Second)
	http.Handle("/metrics", promhttp.InstrumentMetricHandler(
		registry, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	))
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

func upsertData(ctx context.Context, c table.Client, prefix, tableName string, registry *prometheus.Registry, concurrency int) (err error) {
	err = c.RetryIdempotent(ctx, func(ctx context.Context, s table.Session) (err error) {
		return s.CreateTable(ctx, path.Join(prefix, tableName),
			options.WithColumn("series_id", types.Optional(types.TypeUint64)),
			options.WithColumn("title", types.Optional(types.TypeUTF8)),
			options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
			options.WithColumn("release_date", types.Optional(types.TypeUint64)),
			options.WithColumn("comment", types.Optional(types.TypeUTF8)),
			options.WithPrimaryKeyColumn("series_id"),
		)
	})
	if err != nil {
		panic(err)
	}
	rowsLen := 25000000
	batchSize := 1000
	wg := sync.WaitGroup{}
	counter := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "app",
		Name:      "upsert_count",
	}, []string{"success"})
	_ = registry.Register(counter)
	sema := make(chan struct{}, concurrency)
	for shift := 0; shift < rowsLen; shift += batchSize {
		wg.Add(1)
		sema <- struct{}{}
		go func(prefix, tableName string, shift int) {
			defer func() {
				<-sema
				wg.Done()
			}()
			rows := make([]types.Value, 0, batchSize)
			for i := 0; i < batchSize; i++ {
				rows = append(rows, types.StructValue(
					types.StructFieldValue("series_id", types.Uint64Value(uint64(i+shift+3))),
					types.StructFieldValue("title", types.UTF8Value(fmt.Sprintf("series No. %d title", i+shift+3))),
					types.StructFieldValue("series_info", types.UTF8Value(fmt.Sprintf("series No. %d info", i+shift+3))),
					types.StructFieldValue("release_date", types.Uint64Value(uint64(time.Now().Sub(time.Unix(0, 0))/time.Hour/24))),
					types.StructFieldValue("comment", types.UTF8Value(fmt.Sprintf("series No. %d comment", i+shift+3))),
				))
			}
			err = c.RetryIdempotent(ctx, func(ctx context.Context, session table.Session) (err error) {
				return session.BulkUpsert(
					ctx,
					path.Join(prefix, tableName),
					types.ListValue(rows...),
				)
			})
			success := map[string]string{
				"success": func() string {
					if err == nil {
						return "true"
					}
					return "false"
				}(),
			}
			counter.With(success).Add(float64(batchSize) * 100. / float64(rowsLen))
		}(prefix, tableName, shift)
	}
	wg.Wait()
	return nil
}

func scanSelect(ctx context.Context, c table.Client, prefix string, limit int64) (count uint64, err error) {
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%s");
		$format = DateTime::Format("%%Y-%%m-%%d");
		SELECT
			series_id,
			title,
			$format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date
		FROM series LIMIT %d;`,
		prefix,
		limit,
	)
	err = c.RetryIdempotent(
		ctx,
		func(ctx context.Context, s table.Session) error {
			res, err := s.StreamExecuteScanQuery(
				ctx,
				query,
				table.NewQueryParameters(),
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
					count++
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
		},
	)
	return count, err
}
