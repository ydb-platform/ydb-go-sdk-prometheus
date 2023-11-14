## How to install ydb-go-sdk dashboard

1. Open Grafana UI (http://localhost:3000)
2. Add datasource Prometheus with URL `http://prometheus:9090` (check Ok with button `Save&Test`)
3. Select `Import dashboard`
4. Paste content of ydb_go_sdk.json into field `Import via panel json` && press `Load` && press `Import`