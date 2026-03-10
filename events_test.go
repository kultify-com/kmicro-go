package kmicro

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestStream(t *testing.T, km *KMicro, streamName string, subjects []string) {
	t.Helper()
	js, err := km.JetStream()
	require.NoError(t, err)
	_, err = js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name:     streamName,
		Subjects: subjects,
	})
	require.NoError(t, err)
	t.Cleanup(func() { js.DeleteStream(context.Background(), streamName) })
}

func createTestConsumer(t *testing.T, km *KMicro, streamName string, consumerName string, filterSubject string, opts ...func(*jetstream.ConsumerConfig)) {
	t.Helper()
	js, err := km.JetStream()
	require.NoError(t, err)
	cfg := jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	_, err = js.CreateConsumer(t.Context(), streamName, cfg)
	require.NoError(t, err)
}

func TestEvents(t *testing.T) {
	t.Run("should publish and subscribe to events", func(t *testing.T) {
		ctx := context.Background()
		km := NewKMicro("event_test_svc", "0.0.1",
			WithKnownHeaders([]string{"X-TENANT"}),
			WithEventSubjectPrefix("events"),
		)
		require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
		defer km.Stop()

		createTestStream(t, &km, "EVENTS", []string{"events.>"})
		createTestConsumer(t, &km, "EVENTS", "test-consumer", "events.org-123.order")

		received := make(chan DomainEvent, 1)
		err := km.Subscribe(ctx, "EVENTS", "test-consumer", func(ctx context.Context, event DomainEvent) error {
			received <- event
			return nil
		})
		require.NoError(t, err)

		err = km.Publish(ctx, DomainEvent{
			ID:      "evt-001",
			Domain:  "order",
			Type:    "completed",
			OrgID:   "org-123",
			Payload: []byte(`{"orderId":"123"}`),
		})
		require.NoError(t, err)

		select {
		case event := <-received:
			assert.Equal(t, "evt-001", event.ID)
			assert.Equal(t, "order", event.Domain)
			assert.Equal(t, "completed", event.Type)
			assert.Equal(t, "org-123", event.OrgID)
			assert.JSONEq(t, `{"orderId":"123"}`, string(event.Payload))
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for event")
		}
	})

	t.Run("should propagate custom headers to event handler", func(t *testing.T) {
		ctx := context.Background()
		km := NewKMicro("event_header_svc", "0.0.1",
			WithKnownHeaders([]string{"X-TENANT"}),
			WithEventSubjectPrefix("hdr"),
		)
		require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
		defer km.Stop()

		createTestStream(t, &km, "HEADER_EVENTS", []string{"hdr.>"})
		createTestConsumer(t, &km, "HEADER_EVENTS", "header-consumer", "hdr.org-42.header")

		receivedHeaders := make(chan Headers, 1)
		err := km.Subscribe(ctx, "HEADER_EVENTS", "header-consumer", func(ctx context.Context, event DomainEvent) error {
			h, _ := CustomHeadersFromContext(ctx)
			receivedHeaders <- h
			return nil
		})
		require.NoError(t, err)

		pubCtx := ContextWithCustomHeaders(ctx, Headers{"X-TENANT": "tenant-42"})
		err = km.Publish(pubCtx, DomainEvent{
			ID:      "evt-hdr-001",
			Domain:  "header",
			Type:    "test",
			OrgID:   "org-42",
			Payload: []byte(`{}`),
		})
		require.NoError(t, err)

		select {
		case h := <-receivedHeaders:
			assert.Equal(t, "tenant-42", h["X-TENANT"])
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for event")
		}
	})

	t.Run("should nak on handler error", func(t *testing.T) {
		ctx := context.Background()
		km := NewKMicro("event_nak_svc", "0.0.1",
			WithEventSubjectPrefix("nak"),
		)
		require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
		defer km.Stop()

		createTestStream(t, &km, "NAK_EVENTS", []string{"nak.>"})
		createTestConsumer(t, &km, "NAK_EVENTS", "nak-consumer", "nak.org-1.failure", func(cfg *jetstream.ConsumerConfig) {
			cfg.MaxDeliver = 2
		})

		attempts := make(chan struct{}, 5)
		err := km.Subscribe(ctx, "NAK_EVENTS", "nak-consumer", func(ctx context.Context, event DomainEvent) error {
			attempts <- struct{}{}
			return assert.AnError
		})
		require.NoError(t, err)

		err = km.Publish(ctx, DomainEvent{
			ID:      "evt-nak-001",
			Domain:  "failure",
			Type:    "test",
			OrgID:   "org-1",
			Payload: []byte(`{}`),
		})
		require.NoError(t, err)

		select {
		case <-attempts:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for first attempt")
		}

		select {
		case <-attempts:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for redelivery after nak")
		}
	})

	t.Run("should deduplicate events by ID", func(t *testing.T) {
		ctx := context.Background()
		km := NewKMicro("event_dedup_svc", "0.0.1",
			WithEventSubjectPrefix("dedup"),
		)
		require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))
		defer km.Stop()

		createTestStream(t, &km, "DEDUP_EVENTS", []string{"dedup.>"})
		createTestConsumer(t, &km, "DEDUP_EVENTS", "dedup-consumer", "dedup.org-1.test")

		received := make(chan DomainEvent, 5)
		err := km.Subscribe(ctx, "DEDUP_EVENTS", "dedup-consumer", func(ctx context.Context, event DomainEvent) error {
			received <- event
			return nil
		})
		require.NoError(t, err)

		evt := DomainEvent{
			ID:      "evt-dedup-same-id",
			Domain:  "test",
			Type:    "test",
			OrgID:   "org-1",
			Payload: []byte(`{"attempt":1}`),
		}
		require.NoError(t, km.Publish(ctx, evt))
		require.NoError(t, km.Publish(ctx, evt))

		select {
		case <-received:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for event")
		}

		select {
		case <-received:
			t.Fatal("should not receive duplicate event")
		case <-time.After(500 * time.Millisecond):
		}
	})
}
