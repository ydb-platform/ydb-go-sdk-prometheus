package main

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/ydb-platform/ydb-go-sdk-prometheus"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func main() {
	ctx := context.Background()
	registry := prometheus.NewRegistry()
	db, err := ydb.New(
		ctx,
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithTraceDriver(metrics.Driver(
			registry,
			metrics.WithSeparator("_"),
			metrics.WithDetails(trace.DetailsAll),
		)),
		ydb.WithTraceTable(metrics.Table(
			registry,
			metrics.WithSeparator("_"),
			metrics.WithDetails(trace.DetailsAll),
		)),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()
}
