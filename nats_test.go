package kmicro

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The URL fallback owns the connection it dials, so it owns the reconnect
// policy too. A connection the caller passes in keeps the caller's policy.
func TestStartWithNatsURLReconnectsForever(t *testing.T) {
	ctx := context.Background()
	km := NewKMicro("reconnect-url", "0.0.1", WithLogger(slog.New(slog.DiscardHandler)))
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	defer km.Stop()

	assert.Equal(t, -1, km.Nats.Opts.MaxReconnect)
	assert.Equal(t, nats.DefaultReconnectWait, km.Nats.Opts.ReconnectWait)
}

func TestStartWithNatsURLReportsAClosedConnection(t *testing.T) {
	ctx := context.Background()
	logs := &syncBuffer{}
	km := NewKMicro("reconnect-closed", "0.0.1", WithLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))

	km.Stop()

	require.Eventually(t, func() bool {
		return bytes.Contains(logs.Bytes(), []byte(`"level":"ERROR"`))
	}, 5*time.Second, 10*time.Millisecond, "closing the connection logged nothing at error level: %s", logs.Bytes())
	assert.Contains(t, string(logs.Bytes()), natsURL, "the log does not name the connection")
}

func TestStartWithNatsConnectionLeavesTheCallersOptionsAlone(t *testing.T) {
	ctx := context.Background()
	conn, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer conn.Close()

	km := NewKMicro("reconnect-external", "0.0.1", WithLogger(slog.New(slog.DiscardHandler)))
	require.NoError(t, km.Start(ctx, WithNatsConnection(conn)))
	defer km.Stop()

	assert.Equal(t, nats.DefaultMaxReconnect, conn.Opts.MaxReconnect)
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.buf.Bytes())
}
