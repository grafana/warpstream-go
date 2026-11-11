package wgo

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Bounds on fuzz-generated batches, so a mutated input can't allocate an
// arbitrarily large batch. Varint lengths are zigzag-encoded: the record count
// crosses the offset-delta boundary at 64, and a field crosses the length
// boundaries at 64 and 8192.
const (
	fuzzMaxRecords    = 256
	fuzzMaxHeaders    = 7
	fuzzMaxFieldBytes = 1 << 14
	fuzzMaxBatchBytes = 1 << 20
	fuzzEpochMillis   = 1_700_000_000_000
)

const (
	fuzzFieldNull = iota
	fuzzFieldInput
	fuzzFieldRepeated
	fuzzFieldRandom
)

// FuzzEncodeBatch_RoundTripsAndMatchesEstimate checks that every generated
// batch decodes back to its records and that the batch-size estimate equals
// the uncompressed wire size. The input layout is documented on fuzzRecords.
func FuzzEncodeBatch_RoundTripsAndMatchesEstimate(f *testing.F) {
	seeds := []fuzzSeed{
		{},
		fuzzSeed{}.records(1, 0).empty().empty().headers(1).empty().empty(),
		fuzzSeed{}.records(1, 0).input([]byte("k")).input([]byte("v")).headers(2).input([]byte("h1")).null().input([]byte("h2")).input([]byte("hv")),
		fuzzSeed{}.records(1, 0).null().repeated(fuzzMaxFieldBytes, 'x').headers(0),
		fuzzSeed{}.records(1, 0).null().random(fuzzMaxFieldBytes, 1).headers(0),
	}
	for _, n := range []int{63, 64, 8191, 8192, fuzzMaxFieldBytes} {
		seeds = append(seeds, fuzzSeed{}.records(1, 0).repeated(n, 'k').random(n, uint64(n)).headers(1).repeated(n, 'h').repeated(n, 'v'))
	}

	deltas := fuzzSeed{}.records(9, -1).null().input([]byte("first")).headers(0)
	for _, d := range []int64{-1, 63, 64, -8192, 8192, 1 << 20, -(1 << 34), 1 << 41} {
		deltas = deltas.delta(d).null().input([]byte("next")).headers(0)
	}
	seeds = append(seeds, deltas)

	many := fuzzSeed{}.records(65, 0).null().null().headers(0)
	for i := 1; i < 65; i++ {
		many = many.delta(int64(i%3)).null().repeated(i, byte('a'+i%26)).headers(0)
	}
	seeds = append(seeds, many)

	for _, seed := range seeds {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		records := fuzzRecords(data)
		encoded, uncompressedBytes, compressedBytes := encodeBatch(records)

		rb := decodeRecordBatch(t, encoded)
		require.EqualValues(t, len(records), rb.NumRecords)
		require.EqualValues(t, len(records)-1, rb.LastOffsetDelta)
		require.Equal(t, records[0].Timestamp.UnixMilli(), rb.FirstTimestamp)
		require.Equal(t, maxTimestampMillis(records), rb.MaxTimestamp)

		got := decodeBatch(encoded)
		require.Equal(t, carriedFields(records), carriedFields(got))

		estimate := multiRecordBatchEstimateBytes(records)
		require.Equal(t, int64(actualUncompressedMultiRecordBatchWireSize(records)), estimate)
		require.Equal(t, estimate, int64(recordBatchHeaderBytes+uncompressedBytes))
		// recordBatchHeaderBytes counts the request's 4-byte length prefix, which
		// the encoded batch doesn't carry.
		require.Equal(t, recordBatchHeaderBytes+compressedBytes, 4+len(encoded))
		if rb.Attributes&0x7 == 2 {
			require.Less(t, compressedBytes, uncompressedBytes)
		} else {
			require.Equal(t, uncompressedBytes, compressedBytes)
		}
		if len(records) == 1 {
			require.Equal(t, estimate, singleRecordBatchEstimateBytes(records[0]))
		}
	})
}

// fuzzRecords builds between 1 and fuzzMaxRecords records from data, reading
// zeros once data runs out:
//
//   - 2 bytes: the record count, modulo fuzzMaxRecords, minus one.
//   - 5 bytes: the first timestamp, a signed millisecond offset from fuzzEpochMillis.
//   - Per record: after the first, 6 bytes for a signed millisecond delta from the
//     first timestamp; then the key and value fields; then 1 byte for the header
//     count, modulo fuzzMaxHeaders+1, and each header's key and value fields.
//   - Per field: 1 mode byte; for a non-null field, 2 bytes of length, then the
//     field's bytes, the byte to repeat, or an 8-byte seed for random bytes.
//
// Field lengths are capped at fuzzMaxFieldBytes and the batch's total field
// bytes at fuzzMaxBatchBytes.
func fuzzRecords(data []byte) []*kgo.Record {
	in := fuzzInput(data)
	budget := fuzzMaxBatchBytes
	records := make([]*kgo.Record, 1+int(in.uint(2))%fuzzMaxRecords)
	first := fuzzEpochMillis + in.int(5)
	for i := range records {
		ts := first
		if i > 0 {
			ts += in.int(6)
		}
		r := &kgo.Record{Key: in.field(&budget), Value: in.field(&budget), Timestamp: time.UnixMilli(ts)}
		for range int(in.byte()) % (fuzzMaxHeaders + 1) {
			r.Headers = append(r.Headers, kgo.RecordHeader{Key: string(in.field(&budget)), Value: in.field(&budget)})
		}
		records[i] = r
	}
	return records
}

type fuzzInput []byte

func (in *fuzzInput) byte() byte {
	if len(*in) == 0 {
		return 0
	}
	b := (*in)[0]
	*in = (*in)[1:]
	return b
}

func (in *fuzzInput) uint(n int) uint64 {
	var v uint64
	for range n {
		v = v<<8 | uint64(in.byte())
	}
	return v
}

func (in *fuzzInput) int(n int) int64 {
	shift := 64 - 8*n
	return int64(in.uint(n)<<shift) >> shift
}

func (in *fuzzInput) field(budget *int) []byte {
	mode := in.byte() % 4
	if mode == fuzzFieldNull {
		return nil
	}
	n := min(int(in.uint(2))%(fuzzMaxFieldBytes+1), *budget)
	var b []byte
	switch mode {
	case fuzzFieldInput:
		n = min(n, len(*in))
		b = bytes.Clone((*in)[:n])
		*in = (*in)[n:]
	case fuzzFieldRepeated:
		b = bytes.Repeat([]byte{in.byte()}, n)
	case fuzzFieldRandom:
		var seed [32]byte
		binary.BigEndian.PutUint64(seed[:], in.uint(8))
		b = make([]byte, n)
		_, _ = rand.NewChaCha8(seed).Read(b)
	}
	if b == nil {
		b = []byte{}
	}
	*budget -= len(b)
	return b
}

// fuzzSeed writes the input layout fuzzRecords reads, so seeds can be written
// as records rather than bytes.
type fuzzSeed []byte

func (s fuzzSeed) uint(n int, v uint64) fuzzSeed {
	for i := n - 1; i >= 0; i-- {
		s = append(s, byte(v>>(8*i)))
	}
	return s
}

func (s fuzzSeed) records(n int, firstOffsetMillis int64) fuzzSeed {
	return s.uint(2, uint64(n-1)).uint(5, uint64(firstOffsetMillis))
}

func (s fuzzSeed) delta(millis int64) fuzzSeed { return s.uint(6, uint64(millis)) }

func (s fuzzSeed) headers(n int) fuzzSeed { return append(s, byte(n)) }

func (s fuzzSeed) null() fuzzSeed { return append(s, fuzzFieldNull) }

func (s fuzzSeed) empty() fuzzSeed { return s.input(nil) }

func (s fuzzSeed) input(b []byte) fuzzSeed {
	return append(append(s, fuzzFieldInput).uint(2, uint64(len(b))), b...)
}

func (s fuzzSeed) repeated(n int, b byte) fuzzSeed {
	return append(append(s, fuzzFieldRepeated).uint(2, uint64(n)), b)
}

func (s fuzzSeed) random(n int, seed uint64) fuzzSeed {
	return append(s, fuzzFieldRandom).uint(2, uint64(n)).uint(8, seed)
}

func maxTimestampMillis(records []*kgo.Record) int64 {
	maxTS := records[0].Timestamp.UnixMilli()
	for _, r := range records[1:] {
		maxTS = max(maxTS, r.Timestamp.UnixMilli())
	}
	return maxTS
}

// fuzzBytes keeps null and empty apart: on the wire a null value is a
// tombstone and an empty one is not.
type fuzzBytes struct {
	null bool
	data string
}

type fuzzHeader struct {
	key   string
	value fuzzBytes
}

type fuzzRecord struct {
	key, value fuzzBytes
	headers    []fuzzHeader
	millis     int64
}

func carriedFields(records []*kgo.Record) []fuzzRecord {
	bytesOf := func(b []byte) fuzzBytes { return fuzzBytes{null: b == nil, data: string(b)} }
	out := make([]fuzzRecord, len(records))
	for i, r := range records {
		headers := make([]fuzzHeader, len(r.Headers))
		for j, h := range r.Headers {
			headers[j] = fuzzHeader{key: h.Key, value: bytesOf(h.Value)}
		}
		out[i] = fuzzRecord{key: bytesOf(r.Key), value: bytesOf(r.Value), headers: headers, millis: r.Timestamp.UnixMilli()}
	}
	return out
}
