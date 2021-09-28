package common

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"strings"
)

type GaugeName string

type GaugeType int

const (
	GaugeNameError = GaugeType(iota)
	GaugeNameLatency
	GaugeNameTotal
	GaugeNameBalance
	GaugeNameInFlight
	GaugeNameStatus

	DriverGaugeNameConn
	DriverGaugeNameConnInvoke
	DriverGaugeNameConnStream
	DriverGaugeNameConnStreamRecv
	DriverGaugeNameCluster
	DriverGaugeNamePessimize
	DriverGaugeNameInsert
	DriverGaugeNameUpdate
	DriverGaugeNameRemove
	DriverGaugeNameGet
	DriverGaugeNameGetCredentials
	DriverGaugeNameDiscovery
	DriverGaugeNameDiscoveryEndpoints
)

func defaultName(gaugeType GaugeType) GaugeName {
	switch gaugeType {
	case GaugeNameError:
		return "error"
	case GaugeNameLatency:
		return "latency_ms"
	case GaugeNameTotal:
		return "total"
	case GaugeNameInFlight:
		return "in_flight"
	case GaugeNameBalance:
		return "balance"
	case GaugeNameStatus:
		return "status"
	case DriverGaugeNameConn:
		return "conn"
	case DriverGaugeNameConnInvoke:
		return "invoke"
	case DriverGaugeNameConnStream:
		return "stream"
	case DriverGaugeNameConnStreamRecv:
		return "recv"
	case DriverGaugeNameCluster:
		return "cluster"
	case DriverGaugeNameInsert:
		return "insert"
	case DriverGaugeNameUpdate:
		return "update"
	case DriverGaugeNameRemove:
		return "remove"
	case DriverGaugeNameGet:
		return "get"
	case DriverGaugeNamePessimize:
		return "pessimize"
	case DriverGaugeNameGetCredentials:
		return "get_credentials"
	case DriverGaugeNameDiscovery:
		return "discovery"
	case DriverGaugeNameDiscoveryEndpoints:
		return "endpoints"
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

type Details int

const (
	DriverClusterEvents = 1 << iota
	DriverConnEvents
	DriverCredentialsEvents
	DriverDiscoveryEvents
)

type Config interface {
	Details() Details
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
