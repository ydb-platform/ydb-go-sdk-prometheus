package common

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"strings"
	"sync"
)

type GaugeName string

type GaugeType int

const (
	GaugeNameError = GaugeType(iota)
	GaugeNameAttempts
	GaugeNameLatency
	GaugeNameMin
	GaugeNameMax
	GaugeNameTotal
	GaugeNameBalance
	GaugeNameInFlight
	GaugeNameStatus
	GaugeNameLocal
	GaugeNameSet
	GaugeNameIdempotent
	GaugeNameNonIdempotent

	DriverGaugeName
	DriverGaugeNameConn
	DriverGaugeNameConnDial
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

	TableGaugeName
	TableGaugeNameSession
	TableGaugeNameCreateSession
	TableGaugeNameKeepAlive
	TableGaugeNameDeleteSession
	TableGaugeNameQuery
	TableGaugeNamePrepareData
	TableGaugeNameExecuteData
	TableGaugeNameStream
	TableGaugeNameStreamReadTable
	TableGaugeNameStreamExecuteScan
	TableGaugeNameTransaction
	TableGaugeNameBeginTransaction
	TableGaugeNameCommitTransaction
	TableGaugeNameRollbackTransaction
	TableGaugeNamePool
	TableGaugeNamePoolRetry
	TableGaugeNamePoolCreate
	TableGaugeNamePoolClose
	TableGaugeNamePoolGet
	TableGaugeNamePoolWait
	TableGaugeNamePoolTake
	TableGaugeNamePoolPut
	TableGaugeNamePoolCloseSession
)

func defaultName(gaugeType GaugeType) GaugeName {
	switch gaugeType {
	case GaugeNameError:
		return "error"
	case GaugeNameAttempts:
		return "attempts"
	case GaugeNameLatency:
		return "latency_ms"
	case GaugeNameMin:
		return "min"
	case GaugeNameMax:
		return "max"
	case GaugeNameTotal:
		return "total"
	case GaugeNameInFlight:
		return "in_flight"
	case GaugeNameBalance:
		return "balance"
	case GaugeNameStatus:
		return "status"
	case GaugeNameLocal:
		return "local"
	case GaugeNameSet:
		return "set"
	case GaugeNameIdempotent:
		return "idempotent"
	case GaugeNameNonIdempotent:
		return "non-idempotent"
	case DriverGaugeName:
		return "driver"
	case DriverGaugeNameConn:
		return "conn"
	case DriverGaugeNameConnDial:
		return "dial"
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
	case TableGaugeName:
		return "table"
	case TableGaugeNameSession:
		return "session"
	case TableGaugeNameCreateSession:
		return "create"
	case TableGaugeNameKeepAlive:
		return "keep_alive"
	case TableGaugeNameDeleteSession:
		return "delete"
	case TableGaugeNameQuery:
		return "query"
	case TableGaugeNamePrepareData:
		return "prepare_data"
	case TableGaugeNameExecuteData:
		return "execute_data"
	case TableGaugeNameStream:
		return "stream"
	case TableGaugeNameStreamReadTable:
		return "read_table"
	case TableGaugeNameStreamExecuteScan:
		return "execute_scan"
	case TableGaugeNameTransaction:
		return "transaction"
	case TableGaugeNameBeginTransaction:
		return "begin"
	case TableGaugeNameCommitTransaction:
		return "commit"
	case TableGaugeNameRollbackTransaction:
		return "rollback"
	case TableGaugeNamePool:
		return "pool"
	case TableGaugeNamePoolRetry:
		return "retry"
	case TableGaugeNamePoolCreate:
		return "create"
	case TableGaugeNamePoolClose:
		return "close"
	case TableGaugeNamePoolGet:
		return "get"
	case TableGaugeNamePoolWait:
		return "wait"
	case TableGaugeNamePoolTake:
		return "take"
	case TableGaugeNamePoolPut:
		return "put"
	case TableGaugeNamePoolCloseSession:
		return "close_session"
	default:
		return ""
	}
}

type (
	nameFunc    func(gaugeType GaugeType) GaugeName
	errNameFunc func(err error) GaugeName
	gaugeFunc   func(parts ...GaugeName) Gauge
)

func parseConfig(c Config, scopes ...GaugeType) (gaugeFunc, nameFunc, errNameFunc) {
	name := func(gaugeType GaugeType) GaugeName {
		if n := c.Name(gaugeType); n != nil {
			return GaugeName(*n)
		}
		return defaultName(gaugeType)
	}
	prefix := make([]GaugeName, 0, 1+len(scopes))
	if c.Prefix() != nil {
		prefix = append(prefix, GaugeName(*(c.Prefix())))
	}
	for _, path := range scopes {
		prefix = append(prefix, name(path))
	}
	delimiter := "/"
	if c.Delimiter() != nil {
		delimiter = *c.Delimiter()
	}
	errName := func(err error) GaugeName {
		if n := c.ErrName(err); n != nil {
			return GaugeName(*n)
		}
		return GaugeName(defaultErrName(err, delimiter))
	}
	gauges := make(map[GaugeName]Gauge)
	mtx := sync.Mutex{}
	gauge := func(parts ...GaugeName) Gauge {
		parts = append(prefix, parts...)
		n := c.Join(parts...)
		if n == nil {
			s := defaultJoin(delimiter, parts...)
			n = &s
		}
		mtx.Lock()
		defer mtx.Unlock()
		if gauge, ok := (gauges)[GaugeName(*n)]; ok {
			return gauge
		}
		gauge := c.Gauge(*n)
		(gauges)[GaugeName(*n)] = gauge
		return gauge
	}
	return gauge, name, errName
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
