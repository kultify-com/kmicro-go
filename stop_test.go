package kmicro

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubConsumeContext struct {
	closed   chan struct{}
	stopCall chan struct{}
}

func newStubConsumeContext() *stubConsumeContext {
	return &stubConsumeContext{
		closed:   make(chan struct{}),
		stopCall: make(chan struct{}, 1),
	}
}

func (s *stubConsumeContext) Stop() {
	select {
	case s.stopCall <- struct{}{}:
	default:
	}
}

func (s *stubConsumeContext) Drain() {}

func (s *stubConsumeContext) Closed() <-chan struct{} { return s.closed }

func nodeWithConsumers(t *testing.T, consumers ...jetstream.ConsumeContext) *KMicro {
	t.Helper()
	km := NewKMicro("stop-test", "0.0.1", WithLogger(slog.New(slog.DiscardHandler)))
	km.eventConsumers = consumers
	return &km
}

// Stop must not report a node as stopped while its subscriptions are still
// live: a caller that publishes after Stop returns would otherwise have its
// message delivered to a consumer that discards it unacked.
func TestStopWaitsUntilEveryConsumerHasClosed(t *testing.T) {
	first, second := newStubConsumeContext(), newStubConsumeContext()
	km := nodeWithConsumers(t, first, second)

	returned := make(chan struct{})
	go func() {
		km.Stop()
		close(returned)
	}()

	<-first.stopCall
	<-second.stopCall

	select {
	case <-returned:
		t.Fatal("Stop returned while its consumers were still unsubscribing")
	case <-time.After(100 * time.Millisecond):
	}

	close(first.closed)
	select {
	case <-returned:
		t.Fatal("Stop returned with one consumer still unsubscribing")
	case <-time.After(100 * time.Millisecond):
	}

	close(second.closed)
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop never returned after every consumer closed")
	}
}

// Every consumer is stopped before any of them is waited on, so a shutdown
// costs one timeout rather than one per consumer.
func TestStopSignalsEveryConsumerBeforeWaiting(t *testing.T) {
	first, second := newStubConsumeContext(), newStubConsumeContext()
	km := nodeWithConsumers(t, first, second)

	go km.Stop()

	for _, consumer := range []*stubConsumeContext{first, second} {
		select {
		case <-consumer.stopCall:
		case <-time.After(2 * time.Second):
			t.Fatal("Stop waited on one consumer before signalling the next")
		}
	}
}

// The bound is what keeps an unreachable NATS connection from holding a
// shutdown open for ever. The log line is the only signal an operator gets,
// and it has to say how much was left.
func TestStopGivesUpAfterItsTimeoutAndSaysSo(t *testing.T) {
	var logs bytes.Buffer
	km := NewKMicro("stop-test", "0.0.1",
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
		WithStopTimeout(50*time.Millisecond))
	km.eventConsumers = []jetstream.ConsumeContext{newStubConsumeContext()}

	returned := make(chan struct{})
	start := time.Now()
	go func() {
		km.Stop()
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop never gave up on a consumer that never closed")
	}
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.Contains(t, logs.String(), ErrConsumersStillSubscribed.Error())
	assert.Contains(t, logs.String(), "1 of 1 remaining")
}

func TestStopTimeoutDefaultsWhenUnset(t *testing.T) {
	km := NewKMicro("stop-test", "0.0.1", WithLogger(slog.New(slog.DiscardHandler)))
	assert.Equal(t, defaultStopTimeout, km.stopTimeout)

	configured := NewKMicro("stop-test", "0.0.1",
		WithLogger(slog.New(slog.DiscardHandler)), WithStopTimeout(time.Minute))
	assert.Equal(t, time.Minute, configured.stopTimeout)
}

// A caller with its own deadline gets it honoured, and learns WHICH thing was
// cut short: without a distinguishable error it cannot tell a node left
// subscribed from anything else it is handling.
func TestStopContextReturnsWhenItsContextEnds(t *testing.T) {
	km := nodeWithConsumers(t, newStubConsumeContext())

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := km.StopContext(ctx)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConsumersStillSubscribed)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "1 of 1 remaining")
	assert.Lessf(t, time.Since(start), 2*time.Second,
		"StopContext must end with its context, not with the stop timeout of %s", km.stopTimeout)
}

func TestStopContextReportsNoErrorOnceEveryConsumerHasClosed(t *testing.T) {
	consumer := newStubConsumeContext()
	km := nodeWithConsumers(t, consumer)
	close(consumer.closed)

	require.NoError(t, km.StopContext(context.Background()))
}

// What the contract above buys, against a real server: once Stop has returned,
// a newly published message is left for the next consumer rather than
// delivered to this one and discarded.
//
// This test states the property. It does not prove the race is closed, and its
// green must not be read that way: whether an unbounded Stop loses the race
// depends on how long the unsubscribe takes relative to the publish, which is
// a property of the machine rather than of the code. The falsifiable statement
// of the same contract is TestStopWaitsUntilEveryConsumerHasClosed.
func TestStopLeavesLaterEventsForTheNextConsumer(t *testing.T) {
	ctx := context.Background()
	km := NewKMicro("stop-property", "0.0.1", WithLogger(slog.New(slog.DiscardHandler)))
	require.NoError(t, km.Start(ctx, WithNatsURL(natsURL)))

	const (
		streamName   = "STOP_PROPERTY"
		consumerName = "stop-property"
		subject      = "stopproperty.one"
	)
	createTestStream(t, &km, streamName, []string{"stopproperty.>"})
	createTestConsumer(t, &km, streamName, consumerName, "stopproperty.>")

	handled := make(chan struct{}, 4)
	require.NoError(t, km.Subscribe(ctx, streamName, consumerName,
		func(context.Context, DomainEvent) error {
			handled <- struct{}{}
			return nil
		}))

	js, err := km.JetStream()
	require.NoError(t, err)
	_, err = js.Publish(ctx, subject, []byte(`{"id":"first"}`))
	require.NoError(t, err)
	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("the subscriber never received the first event")
	}

	km.Stop()

	conn, err := nats.Connect(natsURL)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	publisher, err := jetstream.New(conn)
	require.NoError(t, err)
	_, err = publisher.Publish(ctx, subject, []byte(`{"id":"second"}`))
	require.NoError(t, err)

	consumer, err := publisher.Consumer(ctx, streamName, consumerName)
	require.NoError(t, err)
	info, err := consumer.Info(ctx)
	require.NoError(t, err)
	assert.Positivef(t, info.NumPending,
		"a stopped node must leave the event pending; NumPending=%d NumAckPending=%d NumWaiting=%d",
		info.NumPending, info.NumAckPending, info.NumWaiting)
	assert.Empty(t, handled, "a stopped node must not handle a later event")
}
