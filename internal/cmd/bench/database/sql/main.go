package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

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

	fmt.Println("GOMAXPROCS =", runtime.GOMAXPROCS(1))
	fmt.Println("GOMAXPROCS =", runtime.GOMAXPROCS(1))
}

func init() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 500
}

func main() {
	registry := prometheus.NewRegistry()

	ctx := context.Background()

	connectCtx, connectCancel := context.WithTimeout(ctx, 500*time.Second)
	defer connectCancel()

	nativeDriver, err := ydb.Open(connectCtx, ydbURL,
		metrics.WithTraces(registry),
		ydb.WithLogger(log.Default(os.Stdout), trace.DetailsAll, log.WithLogQuery()),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = nativeDriver.Close(ctx)
	}()

	connector, err := ydb.Connector(nativeDriver,
		ydb.WithTablePathPrefix("database/sql/bench"),
		ydb.WithAutoDeclare(),
	)
	if err != nil {
		panic("create connector failed: " + err.Error())
	}

	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	go promPusher(registry)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

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

func promPusher(registry prometheus.Gatherer) {
	pusher := push.New(prometheusURL, "bench")
	pusher.Gatherer(registry)
	for {
		time.Sleep(time.Second)
		if err := pusher.Push(); err != nil {
			fmt.Printf("Push error: %+v", err)
		}
	}
}
