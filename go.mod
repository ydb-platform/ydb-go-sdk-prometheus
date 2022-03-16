module github.com/ydb-platform/ydb-go-sdk-prometheus

go 1.16

require (
	github.com/prometheus/client_golang v1.11.0
	github.com/ydb-platform/ydb-go-sdk-metrics v0.11.0-rc1
	github.com/ydb-platform/ydb-go-sdk/v3 v3.14.5-rc1
)

replace github.com/ydb-platform/ydb-go-sdk-metrics => ../ydb-go-sdk-metrics
