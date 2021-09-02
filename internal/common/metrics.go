package common

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
)

type Gauge interface {
	// Inc increments the counter by 1
	Inc()
	// Dec decrements the counter by 1
	Dec()
	// Set sets the Gauge to an arbitrary value.
	Set(value float64)
}

type Config interface {
	// Gauge makes Gauge by name
	Gauge(name string) Gauge
	// WithTags must return child Config by tags cached by tags
	WithTags(tags map[string]string) Config
	// Tagged must return child Config with prefix
	WithPrefix(prefix string) Config
}

func tags(err error) map[string]string {
	var op *ydb.OpError
	var te *ydb.TransportError
	switch {
	case errors.As(err, &te):
		return map[string]string{
			"code":        fmt.Sprintf("%06d", te.Reason),
			"description": "transport/" + strings.ReplaceAll(te.Reason.String(), " ", "_"),
		}
	case errors.As(err, &op):
		return map[string]string{
			"code":        fmt.Sprintf("%06d", op.Reason),
			"description": "operation/" + strings.ReplaceAll(op.Reason.String(), " ", "_"),
		}
	case errors.Is(err, context.DeadlineExceeded):
		return map[string]string{
			"description": "context/deadline_exceeded",
		}
	case errors.Is(err, table.ErrSessionPoolOverflow):
		return map[string]string{
			"description": "session_pool_overflow",
		}
	default:
		return map[string]string{
			"description": "unknown",
		}
	}
}

// DriverTrace makes DriverTrace with metrics publishing
func DriverTrace(c Config) ydb.DriverTrace {
	var (
		errCounterName        = "ydb_driver_err_counter"
		pessiomizationCounter = c.Gauge("ydb_driver_node_pessimization_counter")
		getCredentialsCounter = c.Gauge("ydb_driver_get_credentials_counter")
		discoveryCounter      = c.Gauge("ydb_driver_discovery_counter")
		endpoints             = c.Gauge("ydb_driver_discovery_endpoints")
		operationCounter      = c.Gauge("ydb_driver_operation_counter")
		operationsInFlight    = c.Gauge("ydb_driver_operations_inflight")
		streamRecvCounter     = c.Gauge("ydb_driver_stream_recv_counter")
		streamsInFlight       = c.Gauge("ydb_driver_streams_inflight")
		streamCounter         = c.Gauge("ydb_driver_stream_counter")
	)
	return ydb.DriverTrace{
		OnDial:    nil,
		OnGetConn: nil,
		OnPessimization: func(info ydb.PessimizationStartInfo) func(ydb.PessimizationDoneInfo) {
			return func(info ydb.PessimizationDoneInfo) {
				pessiomizationCounter.Inc()
				if info.Error != nil {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("pessimization").Inc()
				}
			}
		},
		TrackConnStart: nil,
		TrackConnDone:  nil,
		OnGetCredentials: func(info ydb.GetCredentialsStartInfo) func(ydb.GetCredentialsDoneInfo) {
			return func(info ydb.GetCredentialsDoneInfo) {
				if info.Error == nil {
					getCredentialsCounter.Inc()
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("get_credentials").Inc()
				}
			}
		},
		OnDiscovery: func(info ydb.DiscoveryStartInfo) func(ydb.DiscoveryDoneInfo) {
			return func(info ydb.DiscoveryDoneInfo) {
				if info.Error == nil {
					discoveryCounter.Inc()
					endpoints.Set(float64(len(info.Endpoints)))
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("discovery").Inc()
				}
			}
		},
		OnOperation: func(info ydb.OperationStartInfo) func(ydb.OperationDoneInfo) {
			operationsInFlight.Inc()
			return func(info ydb.OperationDoneInfo) {
				if info.Error == nil {
					operationCounter.Inc()
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("operation").Inc()
				}
				operationsInFlight.Dec()
			}
		},
		OnStream: func(info ydb.StreamStartInfo) func(ydb.StreamDoneInfo) {
			streamsInFlight.Inc()
			return func(info ydb.StreamDoneInfo) {
				if info.Error == nil {
					streamCounter.Inc()
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("stream").Inc()
				}
				streamsInFlight.Dec()
			}
		},
		OnStreamRecv: func(info ydb.StreamRecvStartInfo) func(ydb.StreamRecvDoneInfo) {
			return func(info ydb.StreamRecvDoneInfo) {
				if info.Error == nil {
					streamRecvCounter.Inc()
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("stream_recv").Inc()
				}
			}
		},
	}
}

// ClientTrace makes table.ClientTrace with metrics publishing
func ClientTrace(c Config) table.ClientTrace {
	var (
		errCounterName                = "ydb_table_client_err_counter"
		sessionBalance                = c.Gauge("ydb_table_client_session_balance")
		createSessionCounter          = c.Gauge("ydb_table_client_create_session_counter")
		keepAliveCounter              = c.Gauge("ydb_table_client_keep_alive_counter")
		prepareDataQueryCounter       = c.Gauge("ydb_table_client_prepare_data_query_counter")
		executeDataQueryCounter       = c.Gauge("ydb_table_client_execute_data_query_counter")
		streamReadTableCounter        = c.Gauge("ydb_table_client_stream_read_table_counter")
		streamExecuteScanQueryCounter = c.Gauge("ydb_table_client_stream_execute_scan_query_counter")
		beginTransactionCounter       = c.Gauge("ydb_table_client_begin_transaction_counter")
		commitTransactionCounter      = c.Gauge("ydb_table_client_commit_transaction_counter")
		rollbackTransactionCounter    = c.Gauge("ydb_table_client_rollback_transaction_counter")
		transactionsInFlight          = c.Gauge("ydb_table_client_transactions_in_flight")
	)
	return table.ClientTrace{
		OnCreateSession: func(info table.CreateSessionStartInfo) func(table.CreateSessionDoneInfo) {
			return func(info table.CreateSessionDoneInfo) {
				if info.Error == nil {
					sessionBalance.Inc()
					createSessionCounter.Inc()
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("create_session").Inc()
				}
			}
		},
		OnKeepAlive: func(info table.KeepAliveStartInfo) func(table.KeepAliveDoneInfo) {
			return func(info table.KeepAliveDoneInfo) {
				if info.Error == nil {
					keepAliveCounter.Inc()
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("keep_alive").Inc()
				}
			}
		},
		OnDeleteSession: func(info table.DeleteSessionStartInfo) func(table.DeleteSessionDoneInfo) {
			return func(info table.DeleteSessionDoneInfo) {
				sessionBalance.Dec()
				if info.Error != nil {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("delete_session").Inc()
				}
			}
		},
		OnPrepareDataQuery: func(info table.PrepareDataQueryStartInfo) func(table.PrepareDataQueryDoneInfo) {
			return func(info table.PrepareDataQueryDoneInfo) {
				if info.Error == nil {
					prepareDataQueryCounter.Inc()
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("prepare_data_query").Inc()
				}
			}
		},
		OnExecuteDataQuery: func(info table.ExecuteDataQueryStartInfo) func(table.ExecuteDataQueryDoneInfo) {
			return func(info table.ExecuteDataQueryDoneInfo) {
				if info.Error == nil {
					executeDataQueryCounter.Inc()
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("execute_data_query").Inc()
				}
			}
		},
		OnStreamReadTable: func(info table.StreamReadTableStartInfo) func(table.StreamReadTableDoneInfo) {
			return func(info table.StreamReadTableDoneInfo) {
				if info.Error == nil {
					streamReadTableCounter.Inc()
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("stream_read_table").Inc()
				}
			}
		},
		OnStreamExecuteScanQuery: func(info table.StreamExecuteScanQueryStartInfo) func(table.StreamExecuteScanQueryDoneInfo) {
			return func(info table.StreamExecuteScanQueryDoneInfo) {
				if info.Error == nil {
					streamExecuteScanQueryCounter.Inc()
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("stream_execute_scan_query").Inc()
				}
			}
		},
		OnBeginTransaction: func(info table.BeginTransactionStartInfo) func(table.BeginTransactionDoneInfo) {
			return func(info table.BeginTransactionDoneInfo) {
				if info.Error == nil {
					beginTransactionCounter.Inc()
					transactionsInFlight.Inc()
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("begin_transaction").Inc()
				}
			}
		},
		OnCommitTransaction: func(info table.CommitTransactionStartInfo) func(table.CommitTransactionDoneInfo) {
			return func(info table.CommitTransactionDoneInfo) {
				if info.Error == nil {
					commitTransactionCounter.Inc()
					transactionsInFlight.Dec()
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("commit_transaction").Inc()
				}
			}
		},
		OnRollbackTransaction: func(info table.RollbackTransactionStartInfo) func(table.RollbackTransactionDoneInfo) {
			return func(info table.RollbackTransactionDoneInfo) {
				if info.Error == nil {
					rollbackTransactionCounter.Inc()
					transactionsInFlight.Dec()
				} else {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("rollback_transaction").Inc()
				}
			}
		},
	}
}

// SessionPoolTrace makes table.SessionPoolTrace with metrics publishing
func SessionPoolTrace(c Config) table.SessionPoolTrace {
	errCounterName := "ydb_table_session_pool_err_counter"
	getCounter := c.Gauge("ydb_table_session_pool_get_counter")
	getLatency := c.Gauge("ydb_table_session_pool_get_latency")
	getRetries := c.Gauge("ydb_table_session_pool_get_retries")
	reused := c.Gauge("ydb_table_session_pool_reused_counter")
	wait := c.Gauge("ydb_table_session_pool_waitq")
	return table.SessionPoolTrace{
		OnGet: func(info table.SessionPoolGetStartInfo) func(table.SessionPoolGetDoneInfo) {
			return func(info table.SessionPoolGetDoneInfo) {
				getCounter.Inc()
				getRetries.Set(float64(info.RetryAttempts))
				if info.Error != nil {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("get").Inc()
				}
				getLatency.Set(info.Latency.Seconds())
			}
		},
		OnWait: func(info table.SessionPoolWaitStartInfo) func(table.SessionPoolWaitDoneInfo) {
			wait.Inc()
			return func(info table.SessionPoolWaitDoneInfo) {
				wait.Dec()
				if info.Error != nil {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("wait").Inc()
				}
			}
		},
		OnBusyCheck: func(info table.SessionPoolBusyCheckStartInfo) func(table.SessionPoolBusyCheckDoneInfo) {
			return func(info table.SessionPoolBusyCheckDoneInfo) {
				if info.Reused {
					reused.Inc()
				}
				if info.Error != nil {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("busy_check").Inc()
				}
			}
		},
		OnTake: func(info table.SessionPoolTakeStartInfo) func(table.SessionPoolTakeDoneInfo) {
			return func(info table.SessionPoolTakeDoneInfo) {
				if info.Error != nil {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("take").Inc()
				}
			}
		},
		OnPut: func(info table.SessionPoolPutStartInfo) func(table.SessionPoolPutDoneInfo) {
			return func(info table.SessionPoolPutDoneInfo) {
				if info.Error != nil {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("put").Inc()
				}
			}
		},
		OnClose: func(info table.SessionPoolCloseStartInfo) func(table.SessionPoolCloseDoneInfo) {
			return func(info table.SessionPoolCloseDoneInfo) {
				if info.Error != nil {
					c.WithPrefix(errCounterName).WithTags(tags(info.Error)).Gauge("close").Inc()
				}
			}
		},
	}
}

// RetryTrace makes table.RetryTrace with metrics publishing
func RetryTrace(c Config) table.RetryTrace {
	return table.RetryTrace{}
}
