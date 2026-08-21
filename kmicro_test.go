package kmicro

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testContainerNats "github.com/testcontainers/testcontainers-go/modules/nats"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	setup(ctx)
	code := m.Run()
	teardown()
	os.Exit(code)
}

var natsContainer *testContainerNats.NATSContainer
var natsURL string

func setup(ctx context.Context) {
	var err error
	natsContainer, err = testContainerNats.Run(ctx, "nats:2.11")
	if err != nil {
		log.Fatalf("failed to start NATS container: %s", err)
	}
	uri, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		log.Fatalf("failed to get connection string: %s", err)
	}
	natsURL = uri
}

func teardown() {
	if err := testcontainers.TerminateContainer(natsContainer); err != nil {
		log.Printf("failed to terminate container: %s", err)
	}
}

func TestKMicro(t *testing.T) {

	t.Run("should communicate", func(t *testing.T) {
		serviceName := "test_service"
		km := NewKMicro(serviceName, "0.0.1", WithKnownHeaders([]string{"X-AUTH"}))
		ctx := context.Background()
		require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
		defer km.Stop()

		var action1ReceivedData, action2ReceivedData map[string]interface{}

		g1 := km.AddGroup("test_service")
		km.AddEndpoint(ctx, g1, "action1", func(ctx context.Context, data []byte) ([]byte, error) {
			json.Unmarshal(data, &action1ReceivedData)
			customHeaders, ok := CustomHeadersFromContext(ctx)
			assert.True(t, ok, "it should set custom headers")
			if customHeaders["X-AUTH"] != "abc" {
				t.Error("should set customer header")
			}
			action2Result, err := km.Call(ctx, serviceName+".action2", []byte(`{"foo":"bar"}`))
			if err != nil {
				return nil, err
			}
			return action2Result, nil
		})

		km.AddEndpoint(ctx, g1, "action2", func(ctx context.Context, data []byte) ([]byte, error) {
			json.Unmarshal(data, &action2ReceivedData)
			response, _ := json.Marshal(map[string]string{"ret": "var"})
			return response, nil
		})

		customHeaders := Headers{
			"X-AUTH": "abc",
		}
		ctx = ContextWithCustomHeaders(ctx, customHeaders)
		callResult, err := km.Call(ctx, serviceName+".action1", []byte(`{"hello":"world"}`))
		require.NoError(t, err)

		var callResultData map[string]string
		json.Unmarshal(callResult, &callResultData)

		assert.Equal(t, map[string]string{"ret": "var"}, callResultData)
		assert.Equal(t, map[string]interface{}{"hello": "world"}, action1ReceivedData)
		assert.Equal(t, map[string]interface{}{"foo": "bar"}, action2ReceivedData)
	})

	t.Run("should return correct errors", func(t *testing.T) {
		serviceName := "test_service_error"

		km := NewKMicro(serviceName, "0.0.1", WithKnownHeaders([]string{"X-AUTH"}))

		ctx := context.TODO()
		require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
		defer km.Stop()

		g1 := km.AddGroup(serviceName)
		km.AddEndpoint(ctx, g1, "action1", func(ctx context.Context, data []byte) ([]byte, error) {
			val, err := km.Call(ctx, serviceName+".action2", []byte(`{"foo":"bar"}`))
			log.Printf("got from action2: val %v, err %v", val, err)

			return val, err
		})

		km.AddEndpoint(ctx, g1, "action2", func(ctx context.Context, data []byte) ([]byte, error) {
			return nil, errors.New("some error")
		})

		_, err := km.Call(ctx, serviceName+".action1", []byte(`{"hello":"world"}`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "some error")
	})
}

func TestKMicro_CallDepthGuardStopsACycle(t *testing.T) {
	serviceName := "cycle_service"
	km := NewKMicro(serviceName, "0.0.1", WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	ctx := context.Background()
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	defer km.Stop()

	var invocations atomic.Int64
	guarded := make(chan error, 1)

	hop := func(next string) ServiceHandler {
		return func(ctx context.Context, data []byte) ([]byte, error) {
			invocations.Add(1)
			result, err := km.Call(ctx, serviceName+"."+next, data)
			if errors.Is(err, maxCallDepthErr) {
				select {
				case guarded <- err:
				default:
				}
			}
			return result, err
		}
	}

	g := km.AddGroup(serviceName)
	require.NoError(t, km.AddEndpoint(ctx, g, "a", hop("b")))
	require.NoError(t, km.AddEndpoint(ctx, g, "b", hop("a")))

	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err := km.Call(callCtx, serviceName+".a", []byte(`{}`))
	require.Error(t, err, "a cycle must not answer successfully")

	select {
	case guardErr := <-guarded:
		assert.ErrorIs(t, guardErr, maxCallDepthErr)
	default:
		t.Fatalf("no handler saw maxCallDepthErr; the cycle ended some other way: %v", err)
	}
	assert.Equal(t, int64(20), invocations.Load(), "the cycle must stop at the 20th hop")
}

func TestKMicro_DeadlinePropagation(t *testing.T) {
	km := NewKMicro("dl_service", "0.0.1")
	ctx := context.Background()
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	defer km.Stop()

	var gotDeadline bool
	g := km.AddGroup("dl_service")
	km.AddEndpoint(ctx, g, "check", func(ctx context.Context, _ []byte) ([]byte, error) {
		_, gotDeadline = ctx.Deadline()
		return []byte("ok"), nil
	})

	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := km.Call(callCtx, "dl_service.check", []byte("{}"))
	require.NoError(t, err)
	assert.True(t, gotDeadline, "server handler ctx must inherit the caller deadline")
}

func TestKMicro_EndpointInterceptor(t *testing.T) {
	km := NewKMicro("ic_service", "0.0.1", WithKnownHeaders([]string{"X-AUTH"}))
	ctx := context.Background()
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	defer km.Stop()

	deny := func(ctx context.Context, _ []byte) error {
		h, _ := CustomHeadersFromContext(ctx)
		if h["X-AUTH"] != "trusted" {
			return errors.New("untrusted caller")
		}
		return nil
	}
	g := km.AddGroup("ic_service")
	km.AddEndpoint(ctx, g, "guarded", func(ctx context.Context, _ []byte) ([]byte, error) {
		return []byte("ok"), nil
	}, WithInterceptor(deny))

	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := km.Call(cctx, "ic_service.guarded", []byte("{}"))
	require.Error(t, err, "call without trusted X-AUTH must be rejected")
}

// TestKMicro_SequentialRequestsNotCanceledByPriorDeadline guards the endpoint
// handler's per-request context isolation: because each request carries a
// deadline, the handler installs a WithDeadline+cancel. If that derived from a
// context shared across request goroutines, the first request's cancel would
// cancel every later request. Each call here must reach a non-canceled handler.
func TestKMicro_SequentialRequestsNotCanceledByPriorDeadline(t *testing.T) {
	km := NewKMicro("seq_service", "0.0.1")
	ctx := context.Background()
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	defer km.Stop()

	g := km.AddGroup("seq_service")
	require.NoError(t, km.AddEndpoint(ctx, g, "check", func(ctx context.Context, _ []byte) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return []byte("ok"), nil
	}))

	for i := 0; i < 3; i++ {
		callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := km.Call(callCtx, "seq_service.check", []byte("{}"))
		cancel()
		require.NoErrorf(t, err, "call %d must not be canceled by a prior request's deadline", i)
		assert.Equal(t, "ok", string(resp))
	}
}

// TestKMicro_ErroringEndpointDoesNotRaceServiceStats guards the endpoint
// handler's use of micro.Request. nats.go's reqHandler reads req.respondError
// to update endpoint stats as soon as Handle returns, so a Request touched
// after that read is a data race and corrupts the stats it feeds. The handler
// wrapper returns immediately and finishes on its own goroutine, so it must
// answer the caller without going back to the Request. Concurrent callers are
// what make the two accesses overlap; sequential ones do not reproduce it.
// Run with -race.
func TestKMicro_ErroringEndpointDoesNotRaceServiceStats(t *testing.T) {
	km := NewKMicro("race_service", "0.0.1")
	ctx := context.Background()
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	defer km.Stop()

	g := km.AddGroup("race_service")
	require.NoError(t, km.AddEndpoint(ctx, g, "fails", func(ctx context.Context, _ []byte) ([]byte, error) {
		return nil, errors.New("boom")
	}))
	require.NoError(t, km.AddEndpoint(ctx, g, "succeeds", func(ctx context.Context, _ []byte) ([]byte, error) {
		return []byte("ok"), nil
	}))

	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, err := km.Call(cctx, "race_service.fails", []byte("{}"))
				cancel()
				assert.Error(t, err, "handler error must reach the caller")

				cctx, cancel = context.WithTimeout(ctx, 5*time.Second)
				resp, err := km.Call(cctx, "race_service.succeeds", []byte("{}"))
				cancel()
				if assert.NoError(t, err, "successful handler must reach the caller") {
					assert.Equal(t, "ok", string(resp))
				}
			}
		}()
	}
	wg.Wait()
}
