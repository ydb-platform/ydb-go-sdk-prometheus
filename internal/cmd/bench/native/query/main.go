package main

import (
	"context"
	"flag"
	"fmt"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	metrics "github.com/ydb-platform/ydb-go-sdk-prometheus/v2"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

var (
	ydbURL  = "grpc://localhost:2136/local"
	threads = 500
)

func init() {
	flag.StringVar(&ydbURL, "ydb", ydbURL, "connection string for connect to YDB")
	flag.IntVar(&threads, "threads", threads, "concurrency factor for upsert and read data")
}

func isYdbVersionHaveQueryService() error {
	minYdbVersion := strings.Split("24.1", ".")
	ydbVersion := strings.Split(os.Getenv("YDB_VERSION"), ".")
	for i, component := range ydbVersion {
		if i < len(minYdbVersion) {
			if r := strings.Compare(component, minYdbVersion[i]); r < 0 {
				return fmt.Errorf("example '%s' run on minimal YDB version '%v', but current version is '%s'",
					os.Args[0],
					strings.Join(minYdbVersion, "."),
					func() string {
						if len(ydbVersion) > 0 && ydbVersion[0] != "" {
							return strings.Join(ydbVersion, ".")
						}

						return "undefined"
					}(),
				)
			} else if r > 0 {
				return nil
			}
		}
	}

	return nil
}

func main() {
	if err := isYdbVersionHaveQueryService(); err != nil {
		fmt.Println(err.Error())

		return
	}

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
		environ.WithEnvironCredentials(ctx),
		ydb.WithSessionPoolSizeLimit(threads),
		metrics.WithTraces(registry),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	cfg := metrics.Config(registry).WithSystem("app")
	goro := cfg.GaugeVec("goroutines")
	memo := cfg.GaugeVec("memory")
	go func() {
		for {
			time.Sleep(time.Second)
			goro.With(nil).Set(float64(runtime.NumGoroutine()))
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			memo.With(nil).Set(float64(m.HeapSys))
		}
	}()

	prefix := path.Join(db.Name(), "native/query")

	err = createTables(ctx, db.Query(), prefix)
	if err != nil {
		panic(fmt.Errorf("create tables error: %w", err))
	}

	err = fillTablesWithData(ctx, db.Query(), prefix)
	if err != nil {
		panic(fmt.Errorf("fill tables with data error: %w", err))
	}
	if err != nil {
		panic(fmt.Errorf("fill data failed: %w", err))
	}

	wg := &sync.WaitGroup{}
	wg.Add(threads)

	for i := 0; i < threads; i++ {
		go func() {
			defer wg.Done()

			for {
				switch rand.Int31() % 2 {
				case 0:
					_ = read(ctx, db.Query(), prefix)
				case 1:
					_ = readTx(ctx, db.Query(), prefix)
				}
			}
		}()
	}
	wg.Wait()
}
