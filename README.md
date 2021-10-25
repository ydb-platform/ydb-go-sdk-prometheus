# metrics

metrics package helps to create ydb-go-sdk traces with monitoring over prometheus 

## Usage
```go
import (
    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk-prometheus"
)

...
    // init prometheus registry
	registry := prometheus.NewRegistry()

	db, err := ydb.New(
		ctx,
		ydb.MustConnectionString(connection),
		ydb.WithTraceDriver(metrics.Driver(registry)),
		ydb.WithTraceTable(metrics.Table(registry)),
	)

```
