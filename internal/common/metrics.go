package common

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"strings"
	"sync"
)

type GaugeName string

type GaugeType int

const (
	GaugeNameError = GaugeType(iota)

	DriverGaugeNamePessimization
	DriverGaugeNameGetCredentials
	DriverGaugeNameDiscovery
	DriverGaugeNameEndpoints
	DriverGaugeNameOperation
	DriverGaugeNameOperationsInFlight
	DriverGaugeNameStreamRecv
	DriverGaugeNameStreamsInFlight
	DriverGaugeNameStream
)

func defaultName(gaugeType GaugeType) GaugeName {
	switch gaugeType {
	case GaugeNameError:
		return "error"
	case DriverGaugeNamePessimization:
		return "pessimization"
	case DriverGaugeNameGetCredentials:
		return "get_credentials"
	case DriverGaugeNameDiscovery:
		return "discovery"
	case DriverGaugeNameEndpoints:
		return "cluster"
	case DriverGaugeNameOperation:
		return "operation"
	case DriverGaugeNameOperationsInFlight:
		return "operations_in_flight"
	case DriverGaugeNameStreamRecv:
		return "stream_recv"
	case DriverGaugeNameStreamsInFlight:
		return "streams_in_flight"
	case DriverGaugeNameStream:
		return "stream"
	default:
		return ""
	}
}

func defaultJoin(delimiter string, parts ...GaugeName) string {
	s := make([]string, 0, len(parts))
	for _, p := range parts {
		ss := strings.TrimSpace(string(p))
		if ss != "" {
			s = append(s, ss)
		}
	}
	return strings.Join(s, delimiter)
}

func defaultErrName(err error, delimiter string) string {
	if ydb.IsTimeoutError(err) {
		return "timeout"
	}
	if ok, _, text := ydb.IsTransportError(err); ok {
		return strings.Join([]string{"transport", text}, delimiter)
	}
	if ok, _, text := ydb.IsOperationError(err); ok {
		return strings.Join([]string{"operation", text}, delimiter)
	}
	return strings.ReplaceAll(err.Error(), " ", "_")
}

type Gauge interface {
	// Inc increments the counter by 1
	Inc()
	// Dec decrements the counter by 1
	Dec()
	// Set sets the Gauge to an arbitrary value.
	Set(value float64)
	// Value returns current value
	Value() float64
}

type Config interface {
	// Gauge makes Gauge by name
	Gauge(name string) Gauge
	// Delimiter returns delimiter
	Delimiter() *string
	// Prefix returns prefix for gauge or empty string
	Prefix() *string
	// Name returns string name by type
	Name(GaugeType) *string
	// Join returns GaugeName after concatenation
	Join(parts ...GaugeName) *string
	// ErrName returns GaugeName by error
	ErrName(err error) *string
}

// Driver makes Driver with metrics publishing
func Driver(c Config) trace.Driver {
	gauges := make(map[GaugeName]Gauge)
	prefix := GaugeName("")
	if c.Prefix() != nil {
		prefix = GaugeName(*(c.Prefix()))
	}
	delimiter := "/"
	if c.Delimiter() == nil {
		delimiter = *c.Delimiter()
	}
	name := func(gaugeType GaugeType) GaugeName {
		if n := c.Name(gaugeType); n != nil {
			return GaugeName(*n)
		}
		return defaultName(gaugeType)
	}
	errName := func(err error) GaugeName {
		if n := c.ErrName(err); n != nil {
			return GaugeName(*n)
		}
		return GaugeName(defaultErrName(err, delimiter))
	}
	mtx := sync.Mutex{}
	gauge := func(parts ...GaugeName) Gauge {
		parts = append([]GaugeName{prefix}, parts...)
		n := c.Join(parts...)
		if n == nil {
			s := defaultJoin(delimiter, parts...)
			n = &s
		}
		mtx.Lock()
		defer mtx.Unlock()
		if gauge, ok := gauges[GaugeName(*n)]; ok {
			return gauge
		}
		gauge := c.Gauge(*n)
		gauges[GaugeName(*n)] = gauge
		return gauge
	}
	return trace.Driver{
		OnPessimization: func(info trace.PessimizationStartInfo) func(trace.PessimizationDoneInfo) {
			return func(info trace.PessimizationDoneInfo) {
				gauge(
					name(DriverGaugeNamePessimization),
				).Inc()
				if info.Error != nil {
					gauge(
						name(DriverGaugeNamePessimization),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		},
		OnGetCredentials: func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
			return func(info trace.GetCredentialsDoneInfo) {
				gauge(
					name(DriverGaugeNameGetCredentials),
				).Inc()
				if info.Error != nil {
					gauge(
						name(DriverGaugeNameGetCredentials),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		},
		OnDiscovery: func(info trace.DiscoveryStartInfo) func(trace.DiscoveryDoneInfo) {
			return func(info trace.DiscoveryDoneInfo) {
				gauge(
					name(DriverGaugeNameDiscovery),
				).Inc()
				if info.Error != nil {
					gauge(
						name(DriverGaugeNameDiscovery),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				} else {
					gauge(
						name(DriverGaugeNameEndpoints),
					).Set(float64(len(info.Endpoints)))
				}
			}
		},
		OnOperation: func(info trace.OperationStartInfo) func(trace.OperationDoneInfo) {
			gauge(
				name(DriverGaugeNameOperationsInFlight),
			).Inc()
			return func(info trace.OperationDoneInfo) {
				if info.Error != nil {
					gauge(
						name(DriverGaugeNameOperation),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
				gauge(
					name(DriverGaugeNameOperation),
				).Dec()
			}
		},
		OnStream: func(info trace.StreamStartInfo) func(info trace.StreamRecvDoneInfo) func(info trace.StreamDoneInfo) {
			gauge(
				name(DriverGaugeNameStreamsInFlight),
			).Inc()
			return func(info trace.StreamRecvDoneInfo) func(info trace.StreamDoneInfo) {
				gauge(
					name(DriverGaugeNameStreamRecv),
				).Inc()
				if info.Error != nil {
					gauge(
						name(DriverGaugeNameStreamRecv),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
				return func(info trace.StreamDoneInfo) {
					gauge(
						name(DriverGaugeNameStream),
					).Inc()
					if info.Error != nil {
						gauge(
							name(DriverGaugeNameStream),
							name(GaugeNameError),
							errName(info.Error),
						).Inc()
					}
					gauge(
						name(DriverGaugeNameStreamsInFlight),
					).Dec()
				}
			}
		},
	}
}

//// ClientTrace makes table.ClientTrace with metrics publishing
//func ClientTrace(c Config) table.ClientTrace {
//	var (
//		errCounterName                = "ydb_table_client_err_counter"
//		sessionBalance                = c.Gauge("ydb_table_client_session_balance")
//		createSessionCounter          = c.Gauge("ydb_table_client_create_session_counter")
//		keepAliveCounter              = c.Gauge("ydb_table_client_keep_alive_counter")
//		prepareDataQueryCounter       = c.Gauge("ydb_table_client_prepare_data_query_counter")
//		executeDataQueryCounter       = c.Gauge("ydb_table_client_execute_data_query_counter")
//		streamReadTableCounter        = c.Gauge("ydb_table_client_stream_read_table_counter")
//		streamExecuteScanQueryCounter = c.Gauge("ydb_table_client_stream_execute_scan_query_counter")
//		beginTransactionCounter       = c.Gauge("ydb_table_client_begin_transaction_counter")
//		commitTransactionCounter      = c.Gauge("ydb_table_client_commit_transaction_counter")
//		rollbackTransactionCounter    = c.Gauge("ydb_table_client_rollback_transaction_counter")
//		transactionsInFlight          = c.Gauge("ydb_table_client_transactions_in_flight")
//	)
//	return table.ClientTrace{
//		OnCreateSession: func(info table.CreateSessionStartInfo) func(table.CreateSessionDoneInfo) {
//			return func(info table.CreateSessionDoneInfo) {
//				if info.Error == nil {
//					sessionBalance.Inc()
//					createSessionCounter.Inc()
//				} else {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("create_session").Inc()
//				}
//			}
//		},
//		OnKeepAlive: func(info table.KeepAliveStartInfo) func(table.KeepAliveDoneInfo) {
//			return func(info table.KeepAliveDoneInfo) {
//				if info.Error == nil {
//					keepAliveCounter.Inc()
//				} else {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("keep_alive").Inc()
//				}
//			}
//		},
//		OnDeleteSession: func(info table.DeleteSessionStartInfo) func(table.DeleteSessionDoneInfo) {
//			return func(info table.DeleteSessionDoneInfo) {
//				sessionBalance.Dec()
//				if info.Error != nil {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("delete_session").Inc()
//				}
//			}
//		},
//		OnPrepareDataQuery: func(info table.PrepareDataQueryStartInfo) func(table.PrepareDataQueryDoneInfo) {
//			return func(info table.PrepareDataQueryDoneInfo) {
//				if info.Error == nil {
//					prepareDataQueryCounter.Inc()
//				} else {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("prepare_data_query").Inc()
//				}
//			}
//		},
//		OnExecuteDataQuery: func(info table.ExecuteDataQueryStartInfo) func(table.ExecuteDataQueryDoneInfo) {
//			return func(info table.ExecuteDataQueryDoneInfo) {
//				if info.Error == nil {
//					executeDataQueryCounter.Inc()
//				} else {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("execute_data_query").Inc()
//				}
//			}
//		},
//		OnStreamReadTable: func(info table.StreamReadTableStartInfo) func(table.StreamReadTableDoneInfo) {
//			return func(info table.StreamReadTableDoneInfo) {
//				if info.Error == nil {
//					streamReadTableCounter.Inc()
//				} else {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("stream_read_table").Inc()
//				}
//			}
//		},
//		OnStreamExecuteScanQuery: func(info table.StreamExecuteScanQueryStartInfo) func(table.StreamExecuteScanQueryDoneInfo) {
//			return func(info table.StreamExecuteScanQueryDoneInfo) {
//				if info.Error == nil {
//					streamExecuteScanQueryCounter.Inc()
//				} else {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("stream_execute_scan_query").Inc()
//				}
//			}
//		},
//		OnBeginTransaction: func(info table.BeginTransactionStartInfo) func(table.BeginTransactionDoneInfo) {
//			return func(info table.BeginTransactionDoneInfo) {
//				if info.Error == nil {
//					beginTransactionCounter.Inc()
//					transactionsInFlight.Inc()
//				} else {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("begin_transaction").Inc()
//				}
//			}
//		},
//		OnCommitTransaction: func(info table.CommitTransactionStartInfo) func(table.CommitTransactionDoneInfo) {
//			return func(info table.CommitTransactionDoneInfo) {
//				if info.Error == nil {
//					commitTransactionCounter.Inc()
//					transactionsInFlight.Dec()
//				} else {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("commit_transaction").Inc()
//				}
//			}
//		},
//		OnRollbackTransaction: func(info table.RollbackTransactionStartInfo) func(table.RollbackTransactionDoneInfo) {
//			return func(info table.RollbackTransactionDoneInfo) {
//				if info.Error == nil {
//					rollbackTransactionCounter.Inc()
//					transactionsInFlight.Dec()
//				} else {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("rollback_transaction").Inc()
//				}
//			}
//		},
//	}
//}
//
//// SessionPoolTrace makes table.SessionPoolTrace with metrics publishing
//func SessionPoolTrace(c Config) table.SessionPoolTrace {
//	errCounterName := "ydb_table_session_pool_err_counter"
//	getCounter := c.Gauge("ydb_table_session_pool_get_counter")
//	getLatency := c.Gauge("ydb_table_session_pool_get_latency")
//	getRetries := c.Gauge("ydb_table_session_pool_get_retries")
//	wait := c.Gauge("ydb_table_session_pool_waitq")
//	return table.SessionPoolTrace{
//		OnGet: func(info table.SessionPoolGetStartInfo) func(table.SessionPoolGetDoneInfo) {
//			return func(info table.SessionPoolGetDoneInfo) {
//				getCounter.Inc()
//				getRetries.Set(float64(info.RetryAttempts))
//				if info.Error != nil {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("get").Inc()
//				}
//				getLatency.Set(info.Latency.Seconds())
//			}
//		},
//		OnWait: func(info table.SessionPoolWaitStartInfo) func(table.SessionPoolWaitDoneInfo) {
//			wait.Inc()
//			return func(info table.SessionPoolWaitDoneInfo) {
//				wait.Dec()
//				if info.Error != nil {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("wait").Inc()
//				}
//			}
//		},
//		OnTake: func(info table.SessionPoolTakeStartInfo) func(info table.SessionPoolTakeWaitInfo) func(info table.SessionPoolTakeDoneInfo) {
//			return func(info table.SessionPoolTakeWaitInfo) func(info table.SessionPoolTakeDoneInfo) {
//				return func(info table.SessionPoolTakeDoneInfo) {
//					if info.Error != nil {
//						c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("take").Inc()
//					}
//				}
//			}
//		},
//		OnPut: func(info table.SessionPoolPutStartInfo) func(table.SessionPoolPutDoneInfo) {
//			return func(info table.SessionPoolPutDoneInfo) {
//				if info.Error != nil {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("put").Inc()
//				}
//			}
//		},
//		OnClose: func(info table.SessionPoolCloseStartInfo) func(table.SessionPoolCloseDoneInfo) {
//			return func(info table.SessionPoolCloseDoneInfo) {
//				if info.Error != nil {
//					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("close").Inc()
//				}
//			}
//		},
//	}
//}
//
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
