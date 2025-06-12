package kmicro

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

func setupLogger(logger *slog.Logger, svcName, svcVersion string) *slog.Logger {
	initAttr := []slog.Attr{slog.Group("service",
		slog.String("name", svcName),
		slog.String("version", svcVersion),
	)}
	return logger.With(initAttr)
}

type KMicroContextHandler struct {
	slog.Handler
	initAttr []slog.Attr
}

func (h KMicroContextHandler) Handle(ctx context.Context, r slog.Record) error {
	// add predefined attrs
	for _, v := range h.initAttr {
		r.AddAttrs(v)
	}
	// dynamic attrs
	if attrs, ok := ctx.Value(slogFields).([]slog.Attr); ok {
		for _, v := range attrs {
			r.AddAttrs(v)
		}
	}
	// extract and add possible otel information from context
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.HasTraceID() {
		r.AddAttrs(slog.String("trace_id", spanCtx.TraceID().String()))
	}
	if spanCtx.HasSpanID() {
		r.AddAttrs(slog.String("span_id", spanCtx.SpanID().String()))
	}

	return h.Handler.Handle(ctx, r)
}

// WithAttrs implements [slog.Handler].
func (h KMicroContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if h.Handler == nil {
		return h
	}
	return KMicroContextHandler{h.Handler.WithAttrs(attrs), h.initAttr}
}

// WithGroup implements [slog.Handler].
func (h KMicroContextHandler) WithGroup(name string) slog.Handler {
	if h.Handler == nil {
		return h
	}
	return KMicroContextHandler{h.Handler.WithGroup(name), h.initAttr}
}
