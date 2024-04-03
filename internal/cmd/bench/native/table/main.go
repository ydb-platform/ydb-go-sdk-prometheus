package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	metrics "github.com/ydb-platform/ydb-go-sdk-prometheus/v2"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	ydbURL  = "grpc://localhost:2136/local"
	threads = 500
)

func init() {
	flag.StringVar(&ydbURL, "ydb", ydbURL, "connection string for connect to YDB")
	flag.IntVar(&threads, "threads", threads, "concurrency factor for upsert and read data")
}

func main() {
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	registry := prometheus.NewRegistry()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics",
			promhttp.HandlerFor(
				registry,
				promhttp.HandlerOpts{
					Registry: registry,
				},
			),
		)
		if err := http.ListenAndServe(":8080", mux); err != nil {
			log.Fatal(err)
		}
	}()

	connectCtx, connectCancel := context.WithTimeout(ctx, 500*time.Second)
	defer connectCancel()

	db, err := ydb.Open(connectCtx, ydbURL,
		ydb.WithSessionPoolSizeLimit(threads*3),
		metrics.WithTraces(registry),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	if err := prepareScheme(ctx, db.Table(), db.Name(), "series"); err != nil {
		panic(err)
	}

	rowsCount := 25000
	batchSize := 1000

	if err := fillData(ctx,
		db.Table(), db.Name(), "series",
		threads, rowsCount, batchSize,
	); err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(threads)

	for i := 0; i < threads; i++ {
		go func() {
			defer wg.Done()
			for {
				time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
				switch rand.Int31() % 4 {
				case 0:
					_, _ = executeDataQuery(ctx, db.Table(), db.Name(), batchSize)
				case 1:
					_, _ = executeScanQuery(ctx, db.Table(), db.Name(), batchSize)
				case 2:
					_, _ = streamReadTable(ctx, db.Table(), db.Name())
				case 3:
					_ = upsertData(ctx,
						db.Table(), db.Name(), "series",
						batchSize, rand.Int()%(rowsCount-batchSize),
					)
				}
			}
		}()
	}
	wg.Wait()
}

func prepareScheme(
	ctx context.Context,
	c table.Client,
	prefix, tableName string,
) (err error) {
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.DropTable(ctx, path.Join(prefix, tableName))
		},
		table.WithIdempotent(),
		table.WithLabel("DropTable"),
	)
	if err != nil {
		return err
	}
	return c.Do(ctx,
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
		table.WithLabel("CreateTable"),
	)
}

func upsertData(
	ctx context.Context,
	c table.Client,
	prefix, tableName string, batchSize, shift int,
) error {
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
	return c.Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			return session.BulkUpsert(
				ctx,
				path.Join(prefix, tableName),
				types.ListValue(rows...),
			)
		},
		table.WithIdempotent(),
		table.WithLabel("BulkUpsert"),
	)
}

func fillData(
	ctx context.Context,
	c table.Client,
	prefix, tableName string,
	threads int,
	rowsCount int,
	batchSize int,
) (err error) {
	wg := sync.WaitGroup{}
	sema := make(chan struct{}, threads)
	for shift := 0; shift < rowsCount; shift += batchSize {
		wg.Add(1)
		sema <- struct{}{}
		go func(prefix, tableName string, shift int) {
			defer func() {
				<-sema
				wg.Done()
			}()
			_ = upsertData(ctx, c, prefix, tableName, batchSize, shift)
		}(prefix, tableName, shift)
	}
	wg.Wait()
	return nil
}

func executeScanQuery(ctx context.Context, c table.Client, prefix string, limit int) (count uint64, err error) {
	var query = fmt.Sprintf(`
		PRAGMA TablePathPrefix("%s");
		SELECT
			series_id,
			title,
			release_date
		FROM series 
		ORDER BY series_id LIMIT %d;`,
		prefix,
		limit,
	)
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			var res result.StreamResult
			count = 0
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
			}()
			var (
				id    *uint64
				title *string
				date  *time.Time
			)
			//fmt.Fprintf(os.Stdout, "> execute scan query:\n")
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
					//fmt.Fprintf(os.Stdout,
					//	"  > %d %s %s\n",
					//	*id, *title, *date,
					//)
				}
			}
			return res.Err()
		},
		table.WithIdempotent(),
		table.WithLabel("StreamExecuteScanQuery"),
	)
	return
}

func streamReadTable(ctx context.Context, c table.Client, prefix string) (count uint64, err error) {
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			count = 0
			res, err := s.StreamReadTable(
				ctx,
				path.Join(prefix, "series"),
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
			//fmt.Fprintf(os.Stdout, "> execute scan query:\n")
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
					//fmt.Fprintf(os.Stdout,
					//	"  > %d %s %s\n",
					//	*id, *title, *date,
					//)
				}
			}
			return res.Err()
		},
		table.WithIdempotent(),
		table.WithLabel("StreamReadTable"),
	)
	return
}

func executeDataQuery(ctx context.Context, c table.Client, prefix string, limit int) (count uint64, err error) {
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
		func(ctx context.Context, tx table.TransactionActor) (err error) {
			count = 0
			res, err := tx.Execute(
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
			//fmt.Fprintf(os.Stdout, "> execute data query:\n")
			for res.NextResultSet(ctx, "series_id", "title", "release_date") {
				for res.NextRow() {
					count++
					err = res.Scan(&id, &title, &date)
					if err != nil {
						return err
					}
					//fmt.Fprintf(os.Stdout,
					//	"  > %d %s %s\n",
					//	*id, *title, *date,
					//)
				}
			}
			return res.Err()
		},
		table.WithIdempotent(),
		table.WithLabel("ExecuteDataQuery"),
	)
	return
}
