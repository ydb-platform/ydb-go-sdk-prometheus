package common

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"sync"
	"time"
)

// Driver makes Driver with metrics publishing
func Driver(c Config) trace.Driver {
	gauges := make(map[GaugeName]Gauge)
	prefix := GaugeName("")
	if c.Prefix() != nil {
		prefix = GaugeName(*(c.Prefix()))
	}
	delimiter := "/"
	if c.Delimiter() != nil {
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
	//states := make(map[string]int)
	//statesMtx := sync.Mutex{}
	t := trace.Driver{}
	if c.Details()&DriverConnEvents != 0 {
		t.OnConnNew = func(info trace.ConnNewInfo) {
			gauge(
				name(DriverGaugeNameConn),
				name(GaugeNameBalance),
			).Inc()
			gauge(
				name(DriverGaugeNameConn),
				name(GaugeNameStatus),
				GaugeName(info.State.String()),
			).Inc()
		}
		t.OnConnClose = func(info trace.ConnCloseInfo) {
			gauge(
				name(DriverGaugeNameConn),
				name(GaugeNameBalance),
			).Dec()
			gauge(
				name(DriverGaugeNameConn),
				name(GaugeNameStatus),
				GaugeName(info.State.String()),
			).Dec()
		}
		t.OnConnStateChange = func(info trace.ConnStateChangeStartInfo) func(trace.ConnStateChangeDoneInfo) {
			gauge(
				name(DriverGaugeNameConn),
				name(GaugeNameStatus),
				GaugeName(info.State.String()),
			).Dec()
			endpoint := info.Endpoint.Address()
			return func(info trace.ConnStateChangeDoneInfo) {
				gauge(
					name(DriverGaugeNameConn),
					name(GaugeNameStatus),
					GaugeName(info.State.String()),
				).Inc()
				gauge(
					name(DriverGaugeNameConn),
					GaugeName(endpoint),
					name(GaugeNameStatus),
				).Set(float64(info.State.Code()))
			}
		}
		t.OnConnDial = func(info trace.ConnDialStartInfo) func(trace.ConnDialDoneInfo) {
			endpoint := info.Endpoint
			return func(info trace.ConnDialDoneInfo) {
				if info.Error != nil {
					gauge(
						name(DriverGaugeNameConn),
						GaugeName(endpoint.Address()),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				} else {
					gauge(
						name(DriverGaugeNameConn),
						name(GaugeNameInFlight),
					).Inc()
				}
			}
		}
		t.OnConnDisconnect = func(info trace.ConnDisconnectStartInfo) func(trace.ConnDisconnectDoneInfo) {
			return func(info trace.ConnDisconnectDoneInfo) {
				if info.Error != nil {
					gauge(
						name(DriverGaugeNameConn),
						GaugeName(info.State.String()),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				} else {
					gauge(
						name(DriverGaugeNameConn),
						name(GaugeNameInFlight),
					).Dec()
				}
			}
		}
		t.OnConnInvoke = func(info trace.ConnInvokeStartInfo) func(trace.ConnInvokeDoneInfo) {
			gauge(
				name(DriverGaugeNameConn),
				name(DriverGaugeNameConnInvoke),
				name(GaugeNameTotal),
			).Inc()
			method := info.Method.Name()
			gauge(
				name(DriverGaugeNameConn),
				name(DriverGaugeNameConnInvoke),
				GaugeName(method),
				name(GaugeNameTotal),
			).Inc()
			start := time.Now()
			return func(info trace.ConnInvokeDoneInfo) {
				gauge(
					name(DriverGaugeNameConn),
					name(DriverGaugeNameConnInvoke),
					GaugeName(method),
					name(GaugeNameLatency),
				).Set(float64(time.Since(start).Microseconds()) / 1000.)
				if info.Error != nil {
					gauge(
						name(DriverGaugeNameConn),
						name(DriverGaugeNameConnInvoke),
						GaugeName(method),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnConnNewStream = func(info trace.ConnNewStreamStartInfo) func(trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
			gauge(
				name(DriverGaugeNameConn),
				name(DriverGaugeNameConnStream),
				name(GaugeNameTotal),
			).Inc()
			method := info.Method.Name()
			gauge(
				name(DriverGaugeNameConn),
				name(DriverGaugeNameConnStream),
				GaugeName(method),
				name(GaugeNameTotal),
			).Inc()
			start := time.Now()
			counter := 0
			return func(info trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
				counter++
				gauge(
					name(DriverGaugeNameConn),
					name(DriverGaugeNameConnStream),
					name(DriverGaugeNameConnStreamRecv),
					GaugeName(method),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(DriverGaugeNameConn),
						name(DriverGaugeNameConnStream),
						name(DriverGaugeNameConnStreamRecv),
						GaugeName(method),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
				return func(info trace.ConnNewStreamDoneInfo) {
					gauge(
						name(DriverGaugeNameConn),
						name(DriverGaugeNameConnStream),
						GaugeName(method),
						name(GaugeNameLatency),
					).Set(float64(time.Since(start).Microseconds()) / 1000.)
					gauge(
						name(DriverGaugeNameConn),
						name(DriverGaugeNameConnStream),
						name(DriverGaugeNameConnStreamRecv),
						GaugeName(method),
						name(GaugeNameTotal),
					).Set(float64(counter))
					if info.Error != nil {
						gauge(
							name(DriverGaugeNameConn),
							name(DriverGaugeNameConnStream),
							GaugeName(method),
							name(GaugeNameError),
							errName(info.Error),
						).Inc()
					}
				}
			}
		}
	}
	if c.Details()&DriverDiscoveryEvents != 0 {
		t.OnDiscovery = func(info trace.DiscoveryStartInfo) func(trace.DiscoveryDoneInfo) {
			start := time.Now()
			return func(info trace.DiscoveryDoneInfo) {
				gauge(
					name(DriverGaugeNameDiscovery),
					name(GaugeNameLatency),
				).Set(float64(time.Since(start).Microseconds()) / 1000.0)
				gauge(
					name(DriverGaugeNameDiscovery),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(DriverGaugeNameDiscovery),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				} else {
					gauge(
						name(DriverGaugeNameDiscovery),
						name(DriverGaugeNameDiscoveryEndpoints),
					).Set(float64(len(info.Endpoints)))
				}
			}
		}
	}
	if c.Details()&DriverClusterEvents != 0 {
		t.OnClusterGet = func(info trace.ClusterGetStartInfo) func(trace.ClusterGetDoneInfo) {
			return func(info trace.ClusterGetDoneInfo) {
				gauge(
					name(DriverGaugeNameCluster),
					name(DriverGaugeNameGet),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(DriverGaugeNameCluster),
						name(DriverGaugeNameGet),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnClusterInsert = func(info trace.ClusterInsertStartInfo) func(trace.ClusterInsertDoneInfo) {
			return func(info trace.ClusterInsertDoneInfo) {
				gauge(
					name(DriverGaugeNameCluster),
					name(GaugeNameBalance),
				).Inc()
				gauge(
					name(DriverGaugeNameCluster),
					name(DriverGaugeNameInsert),
					name(GaugeNameTotal),
				).Inc()
			}
		}
		t.OnClusterRemove = func(info trace.ClusterRemoveStartInfo) func(trace.ClusterRemoveDoneInfo) {
			return func(info trace.ClusterRemoveDoneInfo) {
				gauge(
					name(DriverGaugeNameCluster),
					name(GaugeNameBalance),
				).Dec()
				gauge(
					name(DriverGaugeNameCluster),
					name(DriverGaugeNameRemove),
					name(GaugeNameTotal),
				).Inc()
			}
		}
		t.OnClusterUpdate = func(info trace.ClusterUpdateStartInfo) func(trace.ClusterUpdateDoneInfo) {
			return func(info trace.ClusterUpdateDoneInfo) {
				gauge(
					name(DriverGaugeNameCluster),
					name(DriverGaugeNameUpdate),
					name(GaugeNameTotal),
				).Inc()
			}
		}
		t.OnPessimizeNode = func(info trace.PessimizeNodeStartInfo) func(trace.PessimizeNodeDoneInfo) {
			return func(info trace.PessimizeNodeDoneInfo) {
				gauge(
					name(DriverGaugeNameCluster),
					name(DriverGaugeNamePessimize),
					name(GaugeNameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(DriverGaugeNameCluster),
						name(DriverGaugeNamePessimize),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}
	if c.Details()&DriverCredentialsEvents != 0 {
		t.OnGetCredentials = func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
			gauge(
				name(DriverGaugeNameGetCredentials),
				name(GaugeNameTotal),
			).Inc()
			start := time.Now()
			return func(info trace.GetCredentialsDoneInfo) {
				gauge(
					name(DriverGaugeNameGetCredentials),
					name(GaugeNameLatency),
				).Set(float64(time.Since(start).Microseconds()) / 1000.)
				if info.Error != nil {
					gauge(
						name(DriverGaugeNameGetCredentials),
						name(GaugeNameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}

	//return trace.Driver{
	//	OnPessimization: func(info trace.PessimizationStartInfo) func(trace.PessimizationDoneInfo) {
	//		start := time.Now()
	//		before := info.State
	//		return func(info trace.PessimizationDoneInfo) {
	//			gauge(
	//				name(DriverGaugeNamePessimize),
	//				name(GaugeNameLatency),
	//			).Set(float64(time.Since(start).Microseconds()) / 1000.0)
	//			if info.Error != nil {
	//				gauge(
	//					name(DriverGaugeNamePessimize),
	//					name(GaugeNameError),
	//					errName(info.Error),
	//				).Inc()
	//			} else {
	//				gauge(
	//					name(DriverGaugeNamePessimize),
	//					name(GaugeNameTotal),
	//				).Inc()
	//			}
	//			if before != info.State {
	//				gauge(
	//					name(DriverGaugeNameCluster),
	//					GaugeName(before.String()),
	//				).Dec()
	//				gauge(
	//					name(DriverGaugeNameCluster),
	//					GaugeName(info.State.String()),
	//				).Inc()
	//			}
	//		}
	//	},
	//	OnGetCredentials: func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
	//		start := time.Now()
	//		return func(info trace.GetCredentialsDoneInfo) {
	//			gauge(
	//				name(DriverGaugeNameGetCredentials),
	//				name(GaugeNameLatency),
	//			).Set(float64(time.Since(start).Microseconds()) / 1000.0)
	//			gauge(
	//				name(DriverGaugeNameGetCredentials),
	//				name(GaugeNameTotal),
	//			).Inc()
	//			if info.Error != nil {
	//				gauge(
	//					name(DriverGaugeNameGetCredentials),
	//					name(GaugeNameError),
	//					errName(info.Error),
	//				).Inc()
	//			}
	//		}
	//	},
	//	OnDiscovery: func(info trace.DiscoveryStartInfo) func(trace.DiscoveryDoneInfo) {
	//		start := time.Now()
	//		return func(info trace.DiscoveryDoneInfo) {
	//			gauge(
	//				name(DriverGaugeNameDiscovery),
	//				name(GaugeNameLatency),
	//			).Set(float64(time.Since(start).Microseconds()) / 1000.0)
	//			gauge(
	//				name(DriverGaugeNameDiscovery),
	//				name(GaugeNameTotal),
	//			).Inc()
	//			if info.Error != nil {
	//				gauge(
	//					name(DriverGaugeNameDiscovery),
	//					name(GaugeNameError),
	//					errName(info.Error),
	//				).Inc()
	//			} else {
	//				gauge(
	//					name(DriverGaugeNameCluster),
	//					name(GaugeNameTotal),
	//				).Set(float64(len(info.Endpoints)))
	//				statesMtx.Lock()
	//				for state := range states {
	//					states[state] = 0
	//					gauge(
	//						name(DriverGaugeNameCluster),
	//						GaugeName(state),
	//					).Set(0)
	//				}
	//				statesMtx.Unlock()
	//				for endpoint, state := range info.Endpoints {
	//					statesMtx.Lock()
	//					states[state.String()] += 1
	//					statesMtx.Unlock()
	//					gauge(
	//						name(DriverGaugeNameCluster),
	//						GaugeName(state.String()),
	//					).Inc()
	//					gauge(
	//						name(DriverGaugeNameCluster),
	//						GaugeName(endpoint.String()),
	//					).Set(float64(state.Code()))
	//				}
	//			}
	//		}
	//	},
	//	OnOperation: func(info trace.OperationStartInfo) func(trace.OperationDoneInfo) {
	//		start := time.Now()
	//		method := GaugeName(strings.TrimLeft(string(info.Method), "/"))
	//		gauge(
	//			name(DriverGaugeNameOperation),
	//			method,
	//			name(GaugeNameInFlight),
	//		).Inc()
	//		return func(info trace.OperationDoneInfo) {
	//			gauge(
	//				name(DriverGaugeNameOperation),
	//				method,
	//				name(GaugeNameLatency),
	//			).Set(float64(time.Since(start).Microseconds()) / 1000.0)
	//			gauge(
	//				name(DriverGaugeNameOperation),
	//				method,
	//				name(GaugeNameTotal),
	//			).Inc()
	//			if info.Error != nil {
	//				gauge(
	//					name(DriverGaugeNameOperation),
	//					method,
	//					name(GaugeNameError),
	//					errName(info.Error),
	//				).Inc()
	//			}
	//			gauge(
	//				name(DriverGaugeNameOperation),
	//				method,
	//				name(GaugeNameInFlight),
	//			).Dec()
	//		}
	//	},
	//	OnStream: func(info trace.StreamStartInfo) func(info trace.StreamRecvDoneInfo) func(info trace.StreamDoneInfo) {
	//		start := time.Now()
	//		gauge(
	//			name(DriverGaugeNameOperation),
	//			name(DriverGaugeNameStream),
	//			name(GaugeNameInFlight),
	//		).Inc()
	//		return func(info trace.StreamRecvDoneInfo) func(info trace.StreamDoneInfo) {
	//			gauge(
	//				name(DriverGaugeNameStreamRecv),
	//				name(GaugeNameTotal),
	//			).Inc()
	//			if info.Error != nil {
	//				gauge(
	//					name(DriverGaugeNameStreamRecv),
	//					name(GaugeNameError),
	//					errName(info.Error),
	//				).Inc()
	//			}
	//			return func(info trace.StreamDoneInfo) {
	//				gauge(
	//					name(DriverGaugeNameStream),
	//					name(GaugeNameLatency),
	//				).Set(float64(time.Since(start).Microseconds()) / 1000.0)
	//				gauge(
	//					name(DriverGaugeNameStream),
	//					name(GaugeNameTotal),
	//				).Inc()
	//				if info.Error != nil {
	//					gauge(
	//						name(DriverGaugeNameStream),
	//						name(GaugeNameError),
	//						errName(info.Error),
	//					).Inc()
	//				}
	//				gauge(
	//					name(DriverGaugeNameOperation),
	//					name(DriverGaugeNameStream),
	//					name(GaugeNameInFlight),
	//				).Dec()
	//			}
	//		}
	//	},
	//}
	return t
}
