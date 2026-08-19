package kmicro

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bytes"
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

func TestStopReportsNothing(t *testing.T) {
	ctx := context.Background()
	logs := &syncBuffer{}
	km := NewKMicro("reconnect-stop", "0.0.1", WithLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	require.NotNil(t, resolveConnectOptions(t, natsURL).ClosedCB, "there is no closed handler here to stay quiet")

	km.Stop()

	require.Never(t, func() bool {
		return bytes.Contains(logs.Bytes(), []byte(`"level":"ERROR"`))
	}, 500*time.Millisecond, 20*time.Millisecond, "Stop closing its own connection is not a dead bus")
}

// A broker that refuses the connection for good is the case the report exists
// for, and it reaches the handler through the wrapper micro.AddService installs.
func TestAFatalServerErrorReportsTheDeadBus(t *testing.T) {
	ctx := context.Background()
	proxy := newNatsProxy(t, natsURL)
	logs := &syncBuffer{}
	km := NewKMicro("reconnect-fatal", "0.0.1", WithLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	require.NoError(t, km.Start(ctx, WithNatsURL(proxy.URL())))
	defer km.Stop()

	proxy.SendServerError("test fatal")

	require.Eventually(t, func() bool {
		return bytes.Contains(logs.Bytes(), []byte(`"level":"ERROR"`))
	}, 5*time.Second, 10*time.Millisecond, "the closed handler reported nothing")
	assert.Contains(t, string(logs.Bytes()), proxy.URL(), "the log does not name the connection")
	assert.True(t, km.Nats.IsClosed(), "a fatal server error left the connection open")
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

func resolveConnectOptions(t *testing.T, connectURL string) nats.Options {
	t.Helper()

	resolved := nats.GetDefaultOptions()
	for _, opt := range natsConnectOptions(connectURL, slog.New(slog.DiscardHandler)) {
		require.NoError(t, opt(&resolved))
	}
	return resolved
}

// natsProxy pipes a client to the real server and can put a protocol error on
// the wire, which is how a broker refuses a connection for good.
type natsProxy struct {
	backend  string
	listener net.Listener

	mu      sync.Mutex
	clients []net.Conn
}

func newNatsProxy(t *testing.T, backendURL string) *natsProxy {
	t.Helper()

	parsed, err := url.Parse(backendURL)
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	proxy := &natsProxy{backend: parsed.Host, listener: listener}
	t.Cleanup(func() { _ = listener.Close() })
	go proxy.serve()
	return proxy
}

func (p *natsProxy) URL() string { return "nats://" + p.listener.Addr().String() }

func (p *natsProxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.backend)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		p.clients = append(p.clients, client)
		p.mu.Unlock()
		go func() {
			_, _ = io.Copy(server, client)
			_ = server.Close()
		}()
		go func() {
			_, _ = io.Copy(client, server)
			_ = client.Close()
		}()
	}
}

// Nothing else is in flight on an idle connection, so the line cannot
// interleave with traffic the real server sent.
func (p *natsProxy) SendServerError(message string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, client := range p.clients {
		_, _ = client.Write([]byte("-ERR '" + message + "'\r\n"))
	}
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
