module github.com/ydb-platform/ydb-go-sdk-prometheus

go 1.16

require (
	github.com/prometheus/client_golang v1.11.0
	github.com/ydb-platform/ydb-go-sdk-sensors v0.0.0-20211017215626-ab73b50793f3
	github.com/ydb-platform/ydb-go-sdk/v3 v3.0.0-20211017214604-e10eb72a6ed6
)

//replace github.com/ydb-platform/ydb-go-sdk/v3 => ./../ydb-go-sdk-private
//replace github.com/ydb-platform/ydb-go-sdk-sensors => ./../ydb-go-sdk-sensors
