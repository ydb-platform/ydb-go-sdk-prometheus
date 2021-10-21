module github.com/ydb-platform/ydb-go-sdk-prometheus

go 1.16

require (
	github.com/prometheus/client_golang v1.11.0
	github.com/ydb-platform/ydb-go-sdk-metrics v0.0.0-20211021205216-75ab5fffb2d0
	github.com/ydb-platform/ydb-go-sdk/v3 v3.0.0-20211021205004-f9e2282d8db1
)

//replace github.com/ydb-platform/ydb-go-sdk/v3 => ./../ydb-go-sdk-private
//replace github.com/ydb-platform/ydb-go-sdk-metrics => ./../ydb-go-sdk-metrics
