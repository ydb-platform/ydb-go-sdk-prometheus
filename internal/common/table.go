package common

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
