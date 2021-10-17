# metrics_local

metrics_local package helps to create ydb-go-sdk traces with monitoring 
over `github.com/rcrowley/go-metrics` package

## Usage
```go
import (
    metrics "github.com/rcrowley/go-metrics"
    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk-prometheus"
)

...
	ctx := context.Background()
	ctx = ydb.WithDriverTrace(ctx, metrics_local.DriverTrace(metrics.DefaultRegistry))
	ctx = table.WithTraceClient(ctx, metrics_local.ClientTrace(metrics.DefaultRegistry))
	ctx = table.WithSessionPoolTrace(ctx, metrics_local.SessionPoolTrace(metrics.DefaultRegistry))
	ctx = table.WithRetryTrace(ctx, metrics_local.RetryTrace(metrics.DefaultRegistry))
	db, err := ydb.New(
		ctx,
		ydb.MustConnectionString(connection),
		ydb.WithTraceDriver(metrics_local.Driver(metrics.DefaultRegistry)),
		ydb.WithTraceTable(metrics_local.Table(metrics.DefaultRegistry)),
	)

```
For plotting metric charts you may use `github.com/aalpern/go-metrics-charts` and 
`github.com/rcrowley/go-metrics/exp` packages  
```go
import (
    metricscharts "github.com/aalpern/go-metrics-charts"
    metrics "github.com/rcrowley/go-metrics"
    "github.com/rcrowley/go-metrics/exp"
)

...
    exp.Exp(metrics.DefaultRegistry)
    metricscharts.Register()

    _ = http.ListenAndServe(":8080", nil)
```
Metric charts you can see at `http://localhost:8080/debug/metrics/charts` page in your browser