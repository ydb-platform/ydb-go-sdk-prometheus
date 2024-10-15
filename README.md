# ydb-go-sdk-prometheus

ydb-go-sdk-prometheus implements prometheus adapter for ydb-go-sdk/v3/metrics package

## Usage
```go
import (
    "github.com/ydb-platform/ydb-go-sdk/v3"
    ydbPrometheus "github.com/ydb-platform/ydb-go-sdk-prometheus/v2"
)

...
    // init prometheus registry
	registry := prometheus.NewRegistry()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydbPrometheus.WithTraces(registry),
	)

```
