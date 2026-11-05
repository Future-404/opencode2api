package httpx

import (
	"bytes"
	"io"
	"sync/atomic"
	"testing"
)

type trackingReadCloser struct {
	io.Reader
	closed atomic.Bool
}

func (t *trackingReadCloser) Close() error {
	t.closed.Store(true)
	return nil
}

func TestDrainAndClose(t *testing.T) {
	// 1. Nil reader
	DrainAndClose(nil)

	// 2. Small body drained and closed
	small := &trackingReadCloser{Reader: bytes.NewReader([]byte("short string"))}
	DrainAndClose(small)
	if !small.closed.Load() {
		t.Fatalf("expected small body to be closed")
	}

	// 3. Large body bounded to 64 KiB
	largeData := make([]byte, 128<<10) // 128 KiB
	large := &trackingReadCloser{Reader: bytes.NewReader(largeData)}
	DrainAndClose(large)
	if !large.closed.Load() {
		t.Fatalf("expected large body to be closed")
	}
}
