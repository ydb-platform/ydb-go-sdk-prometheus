package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
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
)

var (
	ydbURL  = "grpc://localhost:2136/local"
	threads = 50
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

	nativeDriver, err := ydb.Open(connectCtx, ydbURL,
		metrics.WithTraces(registry),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = nativeDriver.Close(ctx)
	}()

	connector, err := ydb.Connector(nativeDriver,
		ydb.WithTablePathPrefix(path.Join(nativeDriver.Name(), "database/sql/bench")),
		ydb.WithAutoDeclare(),
	)
	if err != nil {
		panic("create connector failed: " + err.Error())
	}

	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	db.SetMaxOpenConns(threads * 3)
	db.SetMaxIdleConns(threads * 3)
	db.SetConnMaxIdleTime(time.Second)

	err = prepareSchema(ctx, db)
	if err != nil {
		panic("create tables error: " + err.Error())
	}

	err = fillTablesWithData(ctx, db)
	if err != nil {
		panic("fill tables with data error: " + err.Error())
	}

	wg := sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					err := fillTablesWithData(ctx, db)
					if err != nil {
						fmt.Println("upsert:", err)
					}
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					err := selectDefault(ctx, db)
					if err != nil {
						fmt.Println("execute:", err)
					}
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					err := selectScan(ctx, db)
					if err != nil {
						fmt.Println("scan:", err)
					}
				}
			}
		}()
	}
	wg.Wait()
}
