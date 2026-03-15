package kmicro

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

type DomainEvent struct {
	ID       string `json:"id"`
	Domain   string `json:"domain"`
	EntityID string `json:"entityId"`
	Type     string `json:"type"`
	OrgID    string `json:"orgId"`
	Payload  []byte `json:"payload"`
}

type EventHandler func(ctx context.Context, event DomainEvent) error

func (km *KMicro) Publish(ctx context.Context, event DomainEvent) error {
	js, err := km.JetStream()
	if err != nil {
		return err
	}

	subject := fmt.Sprintf("%s.%s.%s", km.eventSubjectPrefix, event.OrgID, event.Domain)
	header := make(nats.Header)
	header.Set(nats.MsgIdHdr, event.ID)

	propagator := propagation.TraceContext{}
	ctx, span := km.tracer.Start(ctx, fmt.Sprintf("publish: %s", subject),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(semconv.MessagingDestinationName(subject)),
	)
	defer span.End()
	propagator.Inject(ctx, propagation.HeaderCarrier(header))

	if currHeaders, ok := ctx.Value(CustomCtxHeaders).(Headers); ok {
		for _, k := range km.knownHeaders {
			if val, ok := currHeaders[k]; ok {
				header.Set(k, val)
			}
		}
	}

	metricAttrs := metric.WithAttributes(semconv.MessagingDestinationName(subject))

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal domain event: %w", err)
	}

	km.logger.InfoContext(ctx, "publish event", "subject", subject, "eventId", event.ID)
	_, err = js.PublishMsg(ctx, &nats.Msg{
		Subject: subject,
		Header:  header,
		Data:    data,
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		km.logger.ErrorContext(ctx, fmt.Sprintf("failed to publish event (%s): %s", subject, err.Error()))
		km.eventFailed.Add(ctx, 1, metricAttrs)
		return fmt.Errorf("failed to publish event: %w", err)
	}

	span.SetStatus(codes.Ok, "")
	km.eventPublished.Add(ctx, 1, metricAttrs)
	return nil
}

func (km *KMicro) Subscribe(ctx context.Context, streamName string, consumerName string, handler EventHandler) error {
	js, err := km.JetStream()
	if err != nil {
		return err
	}

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return fmt.Errorf("failed to get stream %s: %w", streamName, err)
	}

	consumer, err := stream.Consumer(ctx, consumerName)
	if err != nil {
		return fmt.Errorf("failed to get consumer %s: %w", consumerName, err)
	}

	metricAttrs := metric.WithAttributes(
		semconv.MessagingDestinationName(streamName),
		attribute.String("messaging.consumer.group.name", consumerName),
	)

	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		start := time.Now()
		propagator := propagation.TraceContext{}
		natsHeaders := msg.Headers()
		handlerCtx := propagator.Extract(ctx, propagation.HeaderCarrier(natsHeaders))

		customHeaders := make(Headers, len(km.knownHeaders))
		for _, k := range km.knownHeaders {
			if val := natsHeaders.Get(k); val != "" {
				customHeaders[k] = val
			}
		}
		handlerCtx = ContextWithCustomHeaders(handlerCtx, customHeaders)

		subject := msg.Subject()
		handlerCtx, span := km.tracer.Start(handlerCtx, fmt.Sprintf("handle event: %s", subject),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				semconv.MessagingDestinationName(subject),
				attribute.String("messaging.consumer.group.name", consumerName),
			),
		)
		defer span.End()

		var event DomainEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			km.logger.ErrorContext(handlerCtx, fmt.Sprintf("failed to unmarshal event (%s): %s", subject, err.Error()))
			km.eventFailed.Add(handlerCtx, 1, metricAttrs)
			msg.Term()
			return
		}

		km.logger.InfoContext(handlerCtx, "handle event", "subject", subject, "consumer", consumerName, "eventId", event.ID)

		err := handler(handlerCtx, event)
		duration := time.Since(start)
		km.eventLatency.Record(handlerCtx, duration.Milliseconds(), metricAttrs)

		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			km.logger.ErrorContext(handlerCtx, fmt.Sprintf("event handler error (%s): %s", subject, err.Error()))
			km.eventFailed.Add(handlerCtx, 1, metricAttrs)
			msg.Nak()
			return
		}

		span.SetStatus(codes.Ok, "")
		km.eventProcessed.Add(handlerCtx, 1, metricAttrs)
		msg.Ack()
		km.logger.InfoContext(handlerCtx, "handled event", "subject", subject, "consumer", consumerName, "duration", time.Since(start).String())
	})
	if err != nil {
		return fmt.Errorf("failed to start consuming from %s/%s: %w", streamName, consumerName, err)
	}

	km.eventConsumers = append(km.eventConsumers, cc)
	return nil
}
