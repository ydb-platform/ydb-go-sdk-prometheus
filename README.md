# go_metrics

go_metrics package helps to create ydb-go-sdk traces with monitoring 
over `github.com/rcrowley/go-metrics` package

## Usage
```go
import (
    metrics "github.com/rcrowley/go-metrics"
    "github.com/ydb-platform/ydb-go-sdk-metrics-go-metrics"
)

...
	ctx := context.Background()
	ctx = ydb.WithDriverTrace(ctx, go_metrics.DriverTrace(metrics.DefaultRegistry))
	ctx = table.WithClientTrace(ctx, go_metrics.ClientTrace(metrics.DefaultRegistry))
	ctx = table.WithSessionPoolTrace(ctx, go_metrics.SessionPoolTrace(metrics.DefaultRegistry))
	ctx = table.WithRetryTrace(ctx, go_metrics.RetryTrace(metrics.DefaultRegistry))
	cluster, err := connect.New(
		ctx,
		connect.MustConnectionString(connection),
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