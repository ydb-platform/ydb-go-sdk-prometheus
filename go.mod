module github.com/ydb-platform/ydb-go-sdk-metrics-local

go 1.16

require (
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/ydb-platform/ydb-go-sdk-metrics v0.0.0-20211008060003-77855607d5d0
	github.com/ydb-platform/ydb-go-sdk/v3 v3.0.0-20211008055720-c6a0b43fab84
)

//replace github.com/ydb-platform/ydb-go-sdk/v3 => ./../ydb-go-sdk-private

//replace github.com/ydb-platform/ydb-go-sdk-metrics => ./../ydb-go-sdk-metrics
