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

// An empty code must not reach the wire: Call decides an error reply IS an
// error by the header being non-empty, so an empty code would turn a handler
// failure into a silent empty success.
func TestKMicro_EmptyCodeStillAnswersAsAFailure(t *testing.T) {
	km := NewKMicro("empty_code_service", "0.0.1")
	ctx := context.Background()
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	defer km.Stop()

	g := km.AddGroup("empty_code_service")
	require.NoError(t, km.AddEndpoint(ctx, g, "fails", func(context.Context, []byte) ([]byte, error) {
		return nil, WithCode("", errors.New("boom"))
	}))

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := km.Call(cctx, "empty_code_service.fails", []byte("{}"))

	require.Error(t, err, "an empty code must not read as success")
	assert.Nil(t, resp)
	code, ok := ErrorCode(err)
	require.True(t, ok)
	assert.Equal(t, ErrorCodeInternal, code)
}

// A code states what THIS handler decided. A handler that passes a downstream
// failure on must not republish the downstream module's code as its own answer.
func TestKMicro_ADownstreamCodeIsNotRepublished(t *testing.T) {
	km := NewKMicro("hop_service", "0.0.1")
	ctx := context.Background()
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	defer km.Stop()

	g := km.AddGroup("hop_service")
	require.NoError(t, km.AddEndpoint(ctx, g, "downstream", func(context.Context, []byte) ([]byte, error) {
		return nil, WithCode(ErrorCodeNotFound, errors.New("gone"))
	}))
	require.NoError(t, km.AddEndpoint(ctx, g, "upstream", func(ctx context.Context, _ []byte) ([]byte, error) {
		_, err := km.Call(ctx, "hop_service.downstream", []byte("{}"))
		return nil, fmt.Errorf("upstream: %w", err)
	}))

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := km.Call(cctx, "hop_service.upstream", []byte("{}"))

	require.Error(t, err)
	code, ok := ErrorCode(err)
	require.True(t, ok)
	assert.Equal(t, ErrorCodeInternal, code, "the upstream handler decided nothing; it must not answer the downstream code")
}

// The forward ingress wraps a Call error around its refusal, so an interceptor
// denial must not inherit whatever the identity check's own dependency answered.
func TestKMicro_AnInterceptorDenialStillAnswersForbidden(t *testing.T) {
	km := NewKMicro("ic_code_service", "0.0.1")
	ctx := context.Background()
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	defer km.Stop()

	g := km.AddGroup("ic_code_service")
	require.NoError(t, km.AddEndpoint(ctx, g, "identity", func(context.Context, []byte) ([]byte, error) {
		return nil, errors.New("auth is down")
	}))
	deny := func(ctx context.Context, _ []byte) error {
		_, err := km.Call(ctx, "ic_code_service.identity", []byte("{}"))
		return fmt.Errorf("caller identity check failed: %w", err)
	}
	require.NoError(t, km.AddEndpoint(ctx, g, "guarded", func(context.Context, []byte) ([]byte, error) {
		return []byte("ok"), nil
	}, WithInterceptor(deny)))

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := km.Call(cctx, "ic_code_service.guarded", []byte("{}"))

	require.Error(t, err)
	code, ok := ErrorCode(err)
	require.True(t, ok)
	assert.Equal(t, ErrorCodeForbidden, code, "a refusal must stay a refusal")
}

// A handler may put its own code anywhere in the chain it returns.
func TestKMicro_AHandlerMayWrapItsOwnCode(t *testing.T) {
	km := NewKMicro("wrap_service", "0.0.1")
	ctx := context.Background()
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
	defer km.Stop()

	g := km.AddGroup("wrap_service")
	require.NoError(t, km.AddEndpoint(ctx, g, "gone", func(context.Context, []byte) ([]byte, error) {
		return nil, fmt.Errorf("read: %w", WithCode(ErrorCodeNotFound, errors.New("gone")))
	}))

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := km.Call(cctx, "wrap_service.gone", []byte("{}"))

	require.Error(t, err)
	code, ok := ErrorCode(err)
	require.True(t, ok)
	assert.Equal(t, ErrorCodeNotFound, code)
}

// The fields are exported so a consumer's test can build what Call returns, so
// a literal missing Wrapped is reachable. It must not panic: the endpoint path
// calls Error() on kmicro's own request goroutine, which has no recover.
func TestCodedErrorsSurviveANilWrapped(t *testing.T) {
	assert.Empty(t, (&CodedError{Code: ErrorCodeNotFound}).Error())
	assert.Empty(t, (&CallError{Code: ErrorCodeNotFound}).Error())

	code, ok := ErrorCode(&CallError{Code: ErrorCodeNotFound})
	assert.True(t, ok)
	assert.Equal(t, ErrorCodeNotFound, code)
}
