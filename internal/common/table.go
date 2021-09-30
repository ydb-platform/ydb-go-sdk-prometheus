package common

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	TableSessionEvents = 1 << iota
	TableQueryEvents
	TableStreamEvents
	TableTransactionEvents
	TablePoolEvents
)

// Table makes trace.ClientTrace with metrics publishing
func Table(c Config) trace.Table {
	t := trace.Table{}
	gauge, name, errName := parseConfig(c, TableGaugeName)
	if c.Details()&TableSessionEvents != 0 {
		t.OnPoolRetry = func(info trace.PoolRetryStartInfo) func(trace.PoolRetryDoneInfo) {
			start := time.Now()
			idempotent := func() GaugeName {
				if info.Idempotent {
					return name(GaugeNameIdempotent)
				}
				return name(GaugeNameNonIdempotent)
			}()
			return func(info trace.PoolRetryDoneInfo) {
				gauge(
					name(TableGaugeNamePool),
					name(TableGaugeNamePoolRetry),
					name(GaugeNameLatency),
				).Set(float64(time.Since(start).Microseconds()) / 1000.0)
				gauge(
					name(TableGaugeNamePool),
					name(TableGaugeNamePoolRetry),
					name(GaugeNameAttempts),
				).Set(float64(info.Attempts))
				gauge(
					name(TableGaugeNamePool),
					name(TableGaugeNamePoolRetry),
					idempotent,
					name(GaugeNameLatency),
				).Set(float64(time.Since(start).Microseconds()) / 1000.0)
				gauge(
					name(TableGaugeNamePool),
					name(TableGaugeNamePoolRetry),
					idempotent,
					name(GaugeNameAttempts),
				).Set(float64(info.Attempts))
				if info.Error != nil {
					gauge(
						name(TableGaugeNamePool),
						name(TableGaugeNamePoolRetry),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
					gauge(
						name(TableGaugeNamePool),
						name(TableGaugeNamePoolRetry),
						idempotent,
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnPoolInit = func(info trace.PoolInitStartInfo) func(trace.PoolInitDoneInfo) {
			return func(info trace.PoolInitDoneInfo) {
				gauge(
					name(TableGaugeNamePool),
					name(GaugeNameMax),
				).Set(float64(info.Limit))
				gauge(
					name(TableGaugeNamePool),
					name(GaugeNameMin),
				).Set(float64(info.KeepAliveMinSize))
			}
		}
		t.OnCreateSession = func(info trace.CreateSessionStartInfo) func(trace.CreateSessionDoneInfo) {
			return func(info trace.CreateSessionDoneInfo) {
				gauge(
					name(TableGaugeNameSession),
					name(TableGaugeNameCreateSession),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableGaugeNameSession),
						name(TableGaugeNameCreateSession),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				} else {
					gauge(
						name(TableGaugeNamePool),
						name(GaugeNameBalance),
					).Inc()
				}
			}
		}
		t.OnKeepAlive = func(info trace.KeepAliveStartInfo) func(trace.KeepAliveDoneInfo) {
			return func(info trace.KeepAliveDoneInfo) {
				gauge(
					name(TableGaugeNameSession),
					name(TableGaugeNameKeepAlive),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableGaugeNameSession),
						name(TableGaugeNameKeepAlive),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnDeleteSession = func(info trace.DeleteSessionStartInfo) func(trace.DeleteSessionDoneInfo) {
			start := time.Now()
			return func(info trace.DeleteSessionDoneInfo) {
				gauge(
					name(TableGaugeNameSession),
					name(TableGaugeNameDeleteSession),
					name(GaugeNameLatency),
				).Set(float64(time.Since(start).Microseconds()) / 1000.0)
				gauge(
					name(TableGaugeNameSession),
					name(TableGaugeNameDeleteSession),
					name(GaugeNameTotal),
				).Inc()
				gauge(
					name(TableGaugeNamePool),
					name(GaugeNameBalance),
				).Dec()
				if info.Error != nil {
					gauge(
						name(TableGaugeNameSession),
						name(TableGaugeNameDeleteSession),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}
	if c.Details()&TableQueryEvents != 0 {
		t.OnPrepareDataQuery = func(info trace.PrepareDataQueryStartInfo) func(trace.PrepareDataQueryDoneInfo) {
			return func(info trace.PrepareDataQueryDoneInfo) {
				gauge(
					name(TableGaugeNameQuery),
					name(TableGaugeNamePrepareData),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableGaugeNameQuery),
						name(TableGaugeNamePrepareData),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnExecuteDataQuery = func(info trace.ExecuteDataQueryStartInfo) func(trace.ExecuteDataQueryDoneInfo) {
			return func(info trace.ExecuteDataQueryDoneInfo) {
				gauge(
					name(TableGaugeNameQuery),
					name(TableGaugeNameExecuteData),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableGaugeNameQuery),
						name(TableGaugeNameExecuteData),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}
	if c.Details()&TableStreamEvents != 0 {
		t.OnStreamExecuteScanQuery = func(info trace.StreamExecuteScanQueryStartInfo) func(trace.StreamExecuteScanQueryDoneInfo) {
			return func(info trace.StreamExecuteScanQueryDoneInfo) {
				gauge(
					name(TableGaugeNameStream),
					name(TableGaugeNameStreamExecuteScan),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableGaugeNameStream),
						name(TableGaugeNameStreamExecuteScan),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnStreamReadTable = func(info trace.StreamReadTableStartInfo) func(trace.StreamReadTableDoneInfo) {
			return func(info trace.StreamReadTableDoneInfo) {
				gauge(
					name(TableGaugeNameStream),
					name(TableGaugeNameStreamReadTable),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableGaugeNameStream),
						name(TableGaugeNameStreamReadTable),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}
	if c.Details()&TableTransactionEvents != 0 {
		t.OnBeginTransaction = func(info trace.BeginTransactionStartInfo) func(trace.BeginTransactionDoneInfo) {
			return func(info trace.BeginTransactionDoneInfo) {
				gauge(
					name(TableGaugeNameTransaction),
					name(TableGaugeNameBeginTransaction),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableGaugeNameTransaction),
						name(TableGaugeNameBeginTransaction),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnCommitTransaction = func(info trace.CommitTransactionStartInfo) func(trace.CommitTransactionDoneInfo) {
			return func(info trace.CommitTransactionDoneInfo) {
				gauge(
					name(TableGaugeNameTransaction),
					name(TableGaugeNameCommitTransaction),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableGaugeNameTransaction),
						name(TableGaugeNameCommitTransaction),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnRollbackTransaction = func(info trace.RollbackTransactionStartInfo) func(trace.RollbackTransactionDoneInfo) {
			return func(info trace.RollbackTransactionDoneInfo) {
				gauge(
					name(TableGaugeNameTransaction),
					name(TableGaugeNameRollbackTransaction),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableGaugeNameTransaction),
						name(TableGaugeNameRollbackTransaction),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}
	if c.Details()&TablePoolEvents != 0 {
		t.OnPoolCreate = func(info trace.PoolCreateStartInfo) func(trace.PoolCreateDoneInfo) {
			return func(info trace.PoolCreateDoneInfo) {
				gauge(
					name(TableGaugeNamePool),
					name(TableGaugeNamePoolCreate),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableGaugeNamePool),
						name(TableGaugeNamePoolCreate),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				} else {
					gauge(
						name(TableGaugeNamePool),
						name(GaugeNameTotal),
					).Inc()
				}
			}
		}
		t.OnPoolClose = func(info trace.PoolCloseStartInfo) func(trace.PoolCloseDoneInfo) {
			return func(info trace.PoolCloseDoneInfo) {
				gauge(
					name(TableGaugeNamePool),
					name(TableGaugeNamePoolClose),
					name(GaugeNameTotal),
				).Inc()
				gauge(
					name(TableGaugeNamePool),
					name(GaugeNameTotal),
				).Dec()
				if info.Error != nil {
					gauge(
						name(TableGaugeNamePool),
						name(TableGaugeNamePoolClose),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnPoolGet = func(info trace.PoolGetStartInfo) func(trace.PoolGetDoneInfo) {
			return func(info trace.PoolGetDoneInfo) {
				gauge(
					name(TableGaugeNamePool),
					name(TableGaugeNamePoolGet),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableGaugeNamePool),
						name(TableGaugeNamePoolGet),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				} else {
					gauge(
						name(TableGaugeNamePool),
						name(GaugeNameInFlight),
					).Inc()
				}
			}
		}
		t.OnPoolWait = func(info trace.PoolWaitStartInfo) func(trace.PoolWaitDoneInfo) {
			return func(info trace.PoolWaitDoneInfo) {
				gauge(
					name(TableGaugeNamePool),
					name(TableGaugeNamePoolWait),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableGaugeNamePool),
						name(TableGaugeNamePoolWait),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnPoolTake = func(info trace.PoolTakeStartInfo) func(info trace.PoolTakeWaitInfo) func(info trace.PoolTakeDoneInfo) {
			return func(info trace.PoolTakeWaitInfo) func(info trace.PoolTakeDoneInfo) {
				return func(info trace.PoolTakeDoneInfo) {
					gauge(
						name(TableGaugeNamePool),
						name(TableGaugeNamePoolTake),
						name(GaugeNameTotal),
					).Inc()
					if info.Error != nil {
						gauge(
							name(TableGaugeNamePool),
							name(TableGaugeNamePoolTake),
							name(GaugeNameError),
							errName(info.Error),
						).Inc()
					} else {
						gauge(
							name(TableGaugeNamePool),
							name(GaugeNameInFlight),
						).Inc()
					}
				}
			}
		}
		t.OnPoolPut = func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
			return func(info trace.PoolPutDoneInfo) {
				gauge(
					name(TableGaugeNamePool),
					name(TableGaugeNamePoolPut),
					name(GaugeNameTotal),
				).Inc()
				gauge(
					name(TableGaugeNamePool),
					name(GaugeNameInFlight),
				).Dec()
				if info.Error != nil {
					gauge(
						name(TableGaugeNamePool),
						name(TableGaugeNamePoolPut),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnPoolCloseSession = func(trace.PoolCloseSessionStartInfo) func(trace.PoolCloseSessionDoneInfo) {
			return func(info trace.PoolCloseSessionDoneInfo) {
				gauge(
					name(TableGaugeNamePool),
					name(TableGaugeNamePoolCloseSession),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableGaugeNamePool),
						name(TableGaugeNamePoolCloseSession),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}
	return t
}
