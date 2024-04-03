# Benchmark for tests prometheus metrics

## Run

```shell
% go build -o bench-table .
% ./bench-table -ydb-url=grpc://localhost:2136/local -prometheus-url=http://localhost:8080 -threads=50 
```