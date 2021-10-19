module github.com/ydb-platform/ydb-go-sdk-prometheus

go 1.16

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.31.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/ydb-platform/ydb-go-sdk-sensors v0.0.0-20211019082522-eeb6a5a8d318
	github.com/ydb-platform/ydb-go-sdk/v3 v3.0.0-20211019082416-1fbbe06f1062
	golang.org/x/net v0.0.0-20211015210444-4f30a5c0130f // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20211015200801-69063c4bb744 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20211018162055-cf77aa76bad2 // indirect
	google.golang.org/grpc v1.41.0 // indirect
)

//replace github.com/ydb-platform/ydb-go-sdk/v3 => ./../ydb-go-sdk-private
//
//replace github.com/ydb-platform/ydb-go-sdk-sensors => ./../ydb-go-sdk-sensors
