package common

//// RetryTrace makes table.RetryTrace with metrics publishing
//func RetryTrace(c Config) trace.RetryTrace {
//	attemptsCounter := c.Gauge("ydb_retry_attempts")
//	latencyCounter := c.Gauge("ydb_retry_latency")
//	return trace.RetryTrace{
//		OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
//			return func(info trace.RetryLoopDoneInfo) {
//				attemptsCounter.Set(float64(info.Attempts))
//				latencyCounter.Set(float64(info.Latency))
//			}
//		},
//	}
//}
