package kmicro

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKMicro(t *testing.T) {
	natsURL := os.Getenv("NATS")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	t.Run("should communicate", func(t *testing.T) {
		// ServiceName
		serviceName := "test_service"

		// Initialize KMicro instance
		km := NewKMicro(serviceName, "0.0.1", WithKnownHeaders([]string{"X-AUTH"}))

		// Start KMicro instance
		ctx := context.Background()
		require.NoError(t, km.Start(ctx, natsURL))
		defer km.Stop()

		// Define variables to capture received data
		var action1ReceivedData, action2ReceivedData map[string]interface{}

		// Add endpoints
		km.AddEndpoint(ctx, "action1", func(ctx context.Context, data []byte) ([]byte, error) {
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

		km.AddEndpoint(ctx, "action2", func(ctx context.Context, data []byte) ([]byte, error) {
			json.Unmarshal(data, &action2ReceivedData)
			response, _ := json.Marshal(map[string]string{"ret": "var"})
			return response, nil
		})

		// Call action1 and assert responses
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
		// ServiceName
		serviceName := "test_service_error"

		// Initialize KMicro instance
		km := NewKMicro(serviceName, "0.0.1", WithKnownHeaders([]string{"X-AUTH"}))

		// Start KMicro instance
		ctx := context.TODO()
		require.NoError(t, km.Start(ctx, natsURL))
		defer km.Stop()

		// Add endpoints
		km.AddEndpoint(ctx, "action1", func(ctx context.Context, data []byte) ([]byte, error) {
			val, err := km.Call(ctx, serviceName+".action2", []byte(`{"foo":"bar"}`))
			log.Printf("got from action2: val %v, err %v", val, err)

			return val, err
		})

		km.AddEndpoint(ctx, "action2", func(ctx context.Context, data []byte) ([]byte, error) {
			return nil, errors.New("some error")
		})

		// Call action1 and expect error
		_, err := km.Call(ctx, serviceName+".action1", []byte(`{"hello":"world"}`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "some error") // or more specific error checking
	})
}
