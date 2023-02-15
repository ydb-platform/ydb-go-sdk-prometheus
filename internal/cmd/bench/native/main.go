package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"

	metrics "github.com/ydb-platform/ydb-go-sdk-prometheus"
)

var (
	ydbURL        = "grpc://localhost:2136/local"
	prometheusURL = "http://localhost:8080"
	threads       = 50
)

func init() {
	flag.StringVar(&ydbURL, "ydb-url", ydbURL, "connection string for connect to YDB")
	flag.StringVar(&prometheusURL, "prometheus-url", prometheusURL, "Push url for prometheus metrics, set to 'off' for disable")
	flag.IntVar(&threads, "threads", threads, "concurrency factor for upsert and read data")

	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 500
}

func main() {
	flag.Parse()

	ctx := context.Background()
	registry := prometheus.NewRegistry()

	connectCtx, connectCancel := context.WithTimeout(ctx, 5*time.Second)
	defer connectCancel()

	db, err := ydb.Open(connectCtx, ydbURL,
		metrics.WithTraces(registry),
		ydb.WithLogger(log.Default(os.Stdout), trace.DetailsAll, log.WithLogQuery()),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	go promPusher(registry)

	errs := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "app",
		Name:      "errors",
	}, []string{"error"})
	_ = registry.Register(errs)
	errs.With(map[string]string{
		"error": "",
	}).Add(0)

	if err := upsertData(ctx, db.Table(), db.Name(), "series", registry, threads, errs); err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(threads)

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
	for i := 0; i < threads; i++ {
		go func() {
			defer wg.Done()
			for {
				time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
				inFlight.Add(1)
				start := time.Now()
				var f func(ctx context.Context, c table.Client, prefix string, limit int64) (count uint64, err error)
				switch rand.Int31() % 3 {
				case 0:
					f = executeDataQuery
				case 1:
					f = executeScanQuery
				case 2:
					f = streamReadTable
				}
				count, err := f(
					ctx,
					db.Table(),
					db.Name(),
					10000000, //rand.Int63n(25000),
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
				if err != nil {
					errs.With(map[string]string{
						"error": err.Error(),
					}).Add(1)
				}
			}
		}()
	}
	wg.Wait()
}

func promPusher(registry prometheus.Gatherer) {
	pusher := push.New(prometheusURL, "ydb-go-sdk")
	pusher.Gatherer(registry)
	for {
		time.Sleep(time.Second)
		if err := pusher.Push(); err != nil {
			fmt.Fprintf(os.Stderr, "Push error: %+v", err)
		}
	}

}

func upsertData(
	ctx context.Context,
	c table.Client,
	prefix, tableName string,
	registry *prometheus.Registry,
	threads int,
	errs *prometheus.GaugeVec,
) (err error) {
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.DropTable(ctx, path.Join(prefix, tableName))
		},
		table.WithIdempotent(),
	)
	if err != nil {
		errs.With(map[string]string{
			"error": err.Error(),
		}).Add(1)
	}
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(prefix, tableName),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
				options.WithColumn("release_date", types.Optional(types.TypeDate)),
				options.WithColumn("comment", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("series_id"),
			)
		},
		table.WithIdempotent(),
	)
	if err != nil {
		errs.With(map[string]string{
			"error": err.Error(),
		}).Add(1)
	}
	rowsLen := 25000000
	batchSize := 1000
	wg := sync.WaitGroup{}
	counter := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "app",
		Name:      "upsert_count",
	}, []string{"success"})
	_ = registry.Register(counter)
	sema := make(chan struct{}, threads)
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
					types.StructFieldValue("release_date", types.DateValueFromTime(time.Now())),
					types.StructFieldValue("comment", types.UTF8Value(fmt.Sprintf("series No. %d comment", i+shift+3))),
				))
			}
			err := c.Do(ctx,
				func(ctx context.Context, session table.Session) (err error) {
					return session.BulkUpsert(
						ctx,
						path.Join(prefix, tableName),
						types.ListValue(rows...),
					)
				},
				table.WithIdempotent(),
			)
			if err != nil {
				errs.With(map[string]string{
					"error": err.Error(),
				}).Add(1)
			}
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

func executeScanQuery(ctx context.Context, c table.Client, prefix string, limit int64) (count uint64, err error) {
	var query = fmt.Sprintf(`
		PRAGMA TablePathPrefix("%s");
		SELECT
			series_id,
			title,
			release_date
		FROM series LIMIT %d;`,
		prefix,
		limit,
	)
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			var res result.StreamResult
			count = 0
			start := time.Now()
			res, err = s.StreamExecuteScanQuery(
				ctx,
				query,
				table.NewQueryParameters(),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
				if time.Since(start) > time.Minute {
					fmt.Println(count)
				}
			}()
			var (
				id    *uint64
				title *string
				date  *time.Time
			)
			fmt.Fprintf(os.Stdout, "> execute scan query:\n")
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					count++
					err = res.ScanNamed(
						named.Optional("series_id", &id),
						named.Optional("title", &title),
						named.Optional("release_date", &date),
					)
					if err != nil {
						return err
					}
					fmt.Fprintf(os.Stdout,
						"  > %d %s %s\n",
						*id, *title, *date,
					)
				}
			}
			return res.Err()
		},
		table.WithIdempotent(),
	)
	return
}

func streamReadTable(ctx context.Context, c table.Client, prefix string, limit int64) (count uint64, err error) {
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			var res result.StreamResult
			count = 0
			start := time.Now()
			res, err = s.StreamReadTable(
				ctx,
				path.Join(prefix, "series"),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
				if time.Since(start) > time.Minute {
					fmt.Println(count)
				}
			}()
			var (
				id    *uint64
				title *string
				date  *time.Time
			)
			fmt.Fprintf(os.Stdout, "> execute scan query:\n")
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					count++
					err = res.ScanNamed(
						named.Optional("series_id", &id),
						named.Optional("title", &title),
						named.Optional("release_date", &date),
					)
					if err != nil {
						return err
					}
					fmt.Fprintf(os.Stdout,
						"  > %d %s %s\n",
						*id, *title, *date,
					)
				}
			}
			return res.Err()
		},
		table.WithIdempotent(),
	)
	return
}

func executeDataQuery(ctx context.Context, c table.Client, prefix string, limit int64) (count uint64, err error) {
	var (
		query = fmt.Sprintf(`
			PRAGMA TablePathPrefix("%s");
			SELECT
				series_id,
				title,
				release_date
			FROM series LIMIT %d;`,
			prefix,
			limit,
		)
	)
	err = c.DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) error {
			var res result.StreamResult
			count = 0
			res, err = tx.Execute(
				ctx,
				query,
				table.NewQueryParameters(),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			var (
				id    *uint64
				title *string
				date  *time.Time
			)
			fmt.Fprintf(os.Stdout, "> execute data query:\n")
			for res.NextResultSet(ctx, "series_id", "title", "release_date") {
				for res.NextRow() {
					count++
					err = res.Scan(&id, &title, &date)
					if err != nil {
						return err
					}
					fmt.Fprintf(os.Stdout,
						"  > %d %s %s\n",
						*id, *title, *date,
					)
				}
			}
			return res.Err()
		},
		table.WithIdempotent(),
	)
	return
}
