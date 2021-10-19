module github.com/ydb-platform/ydb-go-sdk-prometheus

go 1.16

require (
	github.com/prometheus/client_golang v1.11.0
	github.com/ydb-platform/ydb-go-sdk-sensors v0.0.0-20211019082522-eeb6a5a8d318
	github.com/ydb-platform/ydb-go-sdk/v3 v3.0.0-20211019082416-1fbbe06f1062
)

//replace github.com/ydb-platform/ydb-go-sdk/v3 => ./../ydb-go-sdk-private
//
//replace github.com/ydb-platform/ydb-go-sdk-sensors => ./../ydb-go-sdk-sensors
