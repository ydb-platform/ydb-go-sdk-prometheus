package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	ydbPrometheus "github.com/ydb-platform/ydb-go-sdk-prometheus"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	prometheusURL = "http://localhost:8080"
	serviceName   = "bench"
	prefix        = "ydb-go-sdk-prometheus/bench/database-sql"
)

func init() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 500
}

func main() {
	registry := prometheus.NewRegistry()

	ctx := context.Background()

	nativeDriver, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithDiscoveryInterval(5*time.Second),
		ydb.WithIgnoreTruncated(),
		ydbPrometheus.WithTraces(registry,
			ydbPrometheus.WithDetails(trace.DatabaseSQLEvents),
		),
	)
	if err != nil {
		panic("connect error: " + err.Error())
	}
	defer func() { _ = nativeDriver.Close(ctx) }()

	connector, err := ydb.Connector(nativeDriver)
	if err != nil {
		panic("create connector failed: " + err.Error())
	}

	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	go promPusher(registry)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cc, err := ydb.Unwrap(db)
	if err != nil {
		panic("unwrap failed: " + err.Error())
	}

	prefix := path.Join(cc.Name(), prefix)

	err = sugar.RemoveRecursive(ctx, cc, prefix)
	if err != nil {
		panic("remove recursive failed: " + err.Error())
	}

	err = prepareSchema(ctx, db, prefix)
	if err != nil {
		panic("create tables error: " + err.Error())
	}

	err = fillTablesWithData(ctx, db, prefix)
	if err != nil {
		panic("fill tables with data error: " + err.Error())
	}

	wg := sync.WaitGroup{}

	errs := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: serviceName,
		Name:      "errors",
	}, []string{"error", "type"})
	_ = registry.Register(errs)
	errs.With(map[string]string{
		"error": "",
		"type":  "",
	}).Add(0)

	rps := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: serviceName,
		Name:      "rps",
	}, []string{"type", "success"})
	_ = registry.Register(rps)
	rps.With(map[string]string{
		"type":    "",
		"success": "",
	}).Add(0)

	for i := 0; i < 10; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for {
				err := fillTablesWithData(ctx, db, prefix)
				rps.With(map[string]string{
					"type":    "upsert",
					"success": ifStr(err == nil, "true", "false"),
				}).Add(1)
				if err != nil {
					fmt.Println("upsert:", err)
					var e interface{ Name() string }
					if errors.As(err, &e) {
						errs.With(map[string]string{
							"error": e.Name(),
							"type":  "upsert",
						}).Add(1)
					} else {
						errs.With(map[string]string{
							"error": err.Error(),
							"type":  "upsert",
						}).Add(1)
					}
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				err := selectDefault(ctx, db, prefix)
				rps.With(map[string]string{
					"type":    "execute",
					"success": ifStr(err == nil, "true", "false"),
				}).Add(1)
				if err != nil {
					fmt.Println("execute:", err)
					var e interface{ Name() string }
					if errors.As(err, &e) {
						errs.With(map[string]string{
							"error": e.Name(),
							"type":  "execute",
						}).Add(1)
					} else {
						errs.With(map[string]string{
							"error": err.Error(),
							"type":  "execute",
						}).Add(1)
					}
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				err := selectScan(ctx, db, prefix)
				rps.With(map[string]string{
					"type":    "scan",
					"success": ifStr(err == nil, "true", "false"),
				}).Add(1)
				if err != nil {
					fmt.Println("scan:", err)
					var e interface{ Name() string }
					if errors.As(err, &e) {
						errs.With(map[string]string{
							"error": e.Name(),
							"type":  "scan",
						}).Add(1)
					} else {
						errs.With(map[string]string{
							"error": err.Error(),
							"type":  "scan",
						}).Add(1)
					}
				}
			}
		}()
	}
	wg.Wait()
}

func promPusher(registry prometheus.Gatherer) {
	pusher := push.New(prometheusURL, serviceName)
	pusher.Gatherer(registry)
	for {
		time.Sleep(time.Second)
		if err := pusher.Push(); err != nil {
			log.Printf("Push error: %+v", err)
		}
	}
}

func ifStr(cond bool, true, false string) string {
	if cond {
		return true
	}
	return false
}
