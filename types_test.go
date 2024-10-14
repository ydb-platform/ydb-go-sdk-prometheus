package metrics

import (
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"testing"
)

func TestWithDetails(t *testing.T) {
	c := Config(nil,
		WithDetails(trace.DriverEvents),
		WithDetails(trace.QueryEvents),
	)
	require.Equal(t, trace.QueryEvents, c.detailer.Details())
}

func TestWithDetailer(t *testing.T) {
	d := trace.DetailsAll
	c := Config(nil,
		WithDetailer(&d),
	)
	require.Equal(t, trace.DetailsAll, c.detailer.Details())
	d = trace.QueryPoolEvents
	require.Equal(t, trace.QueryPoolEvents, c.detailer.Details())
}
