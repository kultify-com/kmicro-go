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

		km.AddEndpoint(ctx, "action1", func(ctx context.Context, data []byte) ([]byte, error) {
			val, err := km.Call(ctx, serviceName+".action2", []byte(`{"foo":"bar"}`))
			log.Printf("got from action2: val %v, err %v", val, err)

			return val, err
		})

		km.AddEndpoint(ctx, "action2", func(ctx context.Context, data []byte) ([]byte, error) {
			return nil, errors.New("some error")
		})

		_, err := km.Call(ctx, serviceName+".action1", []byte(`{"hello":"world"}`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "some error")
	})
}
