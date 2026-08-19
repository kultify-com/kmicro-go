package kmicro

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKMicro_HandlerErrorCodeReachesTheCaller(t *testing.T) {
	km := NewKMicro("code_service", "0.0.1")
	ctx := context.Background()
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	defer km.Stop()

	g := km.AddGroup("code_service")
	require.NoError(t, km.AddEndpoint(ctx, g, "gone", func(context.Context, []byte) ([]byte, error) {
		return nil, WithCode(ErrorCodeNotFound, errors.New("organization not found"))
	}))

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := km.Call(cctx, "code_service.gone", []byte("{}"))

	require.Error(t, err)
	code, ok := ErrorCode(err)
	require.True(t, ok, "the caller must be able to read a code without parsing text")
	assert.Equal(t, ErrorCodeNotFound, code)
}

// A handler that returns a plain error must answer exactly what it answered
// before codes existed. Every endpoint in the estate is that handler today.
func TestKMicro_PlainHandlerErrorStillAnswersInternal(t *testing.T) {
	km := NewKMicro("plain_service", "0.0.1")
	ctx := context.Background()
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	defer km.Stop()

	g := km.AddGroup("plain_service")
	require.NoError(t, km.AddEndpoint(ctx, g, "fails", func(context.Context, []byte) ([]byte, error) {
		return nil, errors.New("boom")
	}))

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := km.Call(cctx, "plain_service.fails", []byte("{}"))

	require.Error(t, err)
	code, ok := ErrorCode(err)
	require.True(t, ok)
	assert.Equal(t, ErrorCodeInternal, code)
}

// A caller that never asks for the code must see byte-for-byte what it saw
// before: the message text alone, formatted the same way.
func TestKMicro_CallerIgnoringTheCodeIsUnaffected(t *testing.T) {
	km := NewKMicro("legacy_service", "0.0.1")
	ctx := context.Background()
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	defer km.Stop()

	g := km.AddGroup("legacy_service")
	require.NoError(t, km.AddEndpoint(ctx, g, "plain", func(context.Context, []byte) ([]byte, error) {
		return nil, errors.New("boom")
	}))
	require.NoError(t, km.AddEndpoint(ctx, g, "coded", func(context.Context, []byte) ([]byte, error) {
		return nil, WithCode(ErrorCodeNotFound, errors.New("boom"))
	}))

	for _, endpoint := range []string{"plain", "coded"} {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := km.Call(cctx, "legacy_service."+endpoint, []byte("{}"))
		cancel()

		require.Error(t, err)
		assert.Equal(t, "action error: boom", err.Error(), endpoint)
	}
}

func TestWithCode(t *testing.T) {
	t.Run("keeps the wrapped error inspectable", func(t *testing.T) {
		sentinel := errors.New("sentinel")
		err := WithCode(ErrorCodeNotFound, fmt.Errorf("read: %w", sentinel))

		assert.ErrorIs(t, err, sentinel)
		assert.Equal(t, "read: sentinel", err.Error())
	})

	t.Run("passes a nil error through", func(t *testing.T) {
		assert.NoError(t, WithCode(ErrorCodeNotFound, nil))
	})

	t.Run("answers no code for an uncoded error", func(t *testing.T) {
		code, ok := ErrorCode(errors.New("plain"))

		assert.False(t, ok)
		assert.Empty(t, code)
	})

	t.Run("answers no code for a nil error", func(t *testing.T) {
		code, ok := ErrorCode(nil)

		assert.False(t, ok)
		assert.Empty(t, code)
	})
}
