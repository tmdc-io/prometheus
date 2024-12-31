package io_prometheus_client

import (
	"bytes"
	"encoding/binary"
	"iter"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
)

const (
	testGauge = `name: "go_build_info"
help: "Build information about the main Go module."
type: GAUGE
metric: <
  label: <
    name: "checksum"
    value: ""
  >
  label: <
    name: "path"
    value: "github.com/prometheus/client_golang"
  >
  label: <
    name: "version"
    value: "(devel)"
  >
  gauge: <
    value: 1
  >
>
metric: <
  label: <
    name: "checksum"
    value: ""
  >
  label: <
    name: "path"
    value: "github.com/prometheus/prometheus"
  >
  label: <
    name: "version"
    value: "v3.0.0"
  >
  gauge: <
    value: 2
  >
>

`
	testCounter = `name: "go_memstats_alloc_bytes_total"
help: "Total number of bytes allocated, even if freed."
type: COUNTER
unit: "bytes"
metric: <
  counter: <
    value: 1.546544e+06
    exemplar: <
      label: <
        name: "dummyID"
        value: "42"
      >
      value: 12
      timestamp: <
        seconds: 1625851151
        nanos: 233181499
      >
    >
  >
>

`
)

func TestMetricDecodingIterator(t *testing.T) {
	varintBuf := make([]byte, binary.MaxVarintLen32)
	buf := bytes.Buffer{}
	for _, m := range []string{testGauge, testCounter} {
		mf := &MetricFamily{}
		require.NoError(t, proto.UnmarshalText(m, mf))
		// From proto message to binary protobuf.
		protoBuf, err := proto.Marshal(mf)
		require.NoError(t, err)

		// Write first length, then binary protobuf.
		varintLength := binary.PutUvarint(varintBuf, uint64(len(protoBuf))) //?
		buf.Write(varintBuf[:varintLength])
		buf.Write(protoBuf)
	}

	t.Run("no errors", func(t *testing.T) {
		// First pass just to check errors.
		iterator := NewMetricDecodingIterator(buf.Bytes())
		for _, err := range iterator {
			require.NoError(t, err)
		}
	})
	t.Run("stop early", func(t *testing.T) {
		iterator := NewMetricDecodingIterator(buf.Bytes())
		nextMetric, stop := iter.Pull2(iterator)

		_, err, ok := nextMetric()
		require.NoError(t, err)
		require.True(t, ok)

		stop()
		_, err, ok = nextMetric()
		require.NoError(t, err)
		require.False(t, ok)
	})
	t.Run("parsed correctly", func(t *testing.T) {
		iterator := NewMetricDecodingIterator(buf.Bytes())
		nextMetric, _ := iter.Pull2(iterator)

		var firstMetricLset labels.Labels
		var firstMetricBytes []byte
		{
			d, err, ok := nextMetric()
			require.NoError(t, err)
			require.True(t, ok)

			require.Equal(t, "go_build_info", d.GetName())
			require.Equal(t, "Build information about the main Go module.", d.GetHelp())
			require.Equal(t, MetricType_GAUGE, d.GetType())

			require.Equal(t, float64(1), d.GetGauge().GetValue())
			require.Equal(t, []byte{0xa, 0xa, 0xa, 0x8, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x73, 0x75, 0x6d, 0xa, 0x2b, 0xa, 0x4, 0x70, 0x61, 0x74, 0x68, 0x12, 0x23, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x70, 0x72, 0x6f, 0x6d, 0x65, 0x74, 0x68, 0x65, 0x75, 0x73, 0x2f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x67, 0x6f, 0x6c, 0x61, 0x6e, 0x67, 0xa, 0x12, 0xa, 0x7, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x7, 0x28, 0x64, 0x65, 0x76, 0x65, 0x6c, 0x29, 0x12, 0x9, 0x9, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f}, d.MetricBytes())
			b := labels.NewScratchBuilder(0)
			require.NoError(t, d.Label(&b))

			firstMetricLset = b.Labels()
			firstMetricBytes = d.MetricBytes()

			require.Equal(t, `{checksum="", path="github.com/prometheus/client_golang", version="(devel)"}`, firstMetricLset.String())
		}

		{
			d, err, ok := nextMetric()
			require.NoError(t, err)
			require.True(t, ok)

			require.Equal(t, "go_build_info", d.GetName())
			require.Equal(t, "Build information about the main Go module.", d.GetHelp())
			require.Equal(t, MetricType_GAUGE, d.GetType())

			require.Equal(t, float64(2), d.GetGauge().GetValue())
			require.Equal(t, []byte{0xa, 0xa, 0xa, 0x8, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x73, 0x75, 0x6d, 0xa, 0x28, 0xa, 0x4, 0x70, 0x61, 0x74, 0x68, 0x12, 0x20, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x70, 0x72, 0x6f, 0x6d, 0x65, 0x74, 0x68, 0x65, 0x75, 0x73, 0x2f, 0x70, 0x72, 0x6f, 0x6d, 0x65, 0x74, 0x68, 0x65, 0x75, 0x73, 0xa, 0x11, 0xa, 0x7, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x6, 0x76, 0x33, 0x2e, 0x30, 0x2e, 0x30, 0x12, 0x9, 0x9, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40}, d.MetricBytes())
			b := labels.NewScratchBuilder(0)
			require.NoError(t, d.Label(&b))
			require.Equal(t, `{checksum="", path="github.com/prometheus/prometheus", version="v3.0.0"}`, b.Labels().String())
		}
		{
			// Different mf now.
			d, err, ok := nextMetric()
			require.NoError(t, err)
			require.True(t, ok)

			require.Equal(t, "go_memstats_alloc_bytes_total", d.GetName())
			require.Equal(t, "Total number of bytes allocated, even if freed.", d.GetHelp())
			require.Equal(t, "bytes", d.GetUnit())
			require.Equal(t, MetricType_COUNTER, d.GetType())

			require.Equal(t, 1.546544e+06, d.Metric.GetCounter().GetValue())
			require.Equal(t, []byte{0x1a, 0x30, 0x9, 0x0, 0x0, 0x0, 0x0, 0x30, 0x99, 0x37, 0x41, 0x12, 0x25, 0xa, 0xd, 0xa, 0x7, 0x64, 0x75, 0x6d, 0x6d, 0x79, 0x49, 0x44, 0x12, 0x2, 0x34, 0x32, 0x11, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x28, 0x40, 0x1a, 0xb, 0x8, 0x8f, 0x8a, 0xa2, 0x87, 0x6, 0x10, 0xbb, 0xa2, 0x98, 0x6f}, d.MetricBytes())
			b := labels.NewScratchBuilder(0)
			require.NoError(t, d.Label(&b))
			require.Equal(t, `{}`, b.Labels().String())
		}

		_, err, ok := nextMetric()
		require.NoError(t, err)
		require.False(t, ok)

		// Expect labels and metricBytes to be static and reusable even after parsing.
		require.Equal(t, []byte{0xa, 0xa, 0xa, 0x8, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x73, 0x75, 0x6d, 0xa, 0x2b, 0xa, 0x4, 0x70, 0x61, 0x74, 0x68, 0x12, 0x23, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x70, 0x72, 0x6f, 0x6d, 0x65, 0x74, 0x68, 0x65, 0x75, 0x73, 0x2f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x67, 0x6f, 0x6c, 0x61, 0x6e, 0x67, 0xa, 0x12, 0xa, 0x7, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x7, 0x28, 0x64, 0x65, 0x76, 0x65, 0x6c, 0x29, 0x12, 0x9, 0x9, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f}, firstMetricBytes)
		require.Equal(t, `{checksum="", path="github.com/prometheus/client_golang", version="(devel)"}`, firstMetricLset.String())
	})

}
