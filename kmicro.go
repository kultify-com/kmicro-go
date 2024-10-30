package kmicro

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type KMicro struct {
	micro.Group
	svcName      string
	svcVersion   string
	useOtel      bool
	nats         *nats.Conn
	natsSvc      micro.Service
	otelShutdown func() error
	tracer       trace.Tracer
	meter        metric.Meter
	knownHeaders []string
	logger       *slog.Logger
}

const (
	callDepthCtxKey = iota
	CustomCtxHeaders
	slogFields
)

var (
	maxCallDepthErr = errors.New("max call depth reached")
)

const headerCallDepthKey = "kmicro_callDepth"

type ServiceHandler func(ctx context.Context, data []byte) ([]byte, error)

func NewKMicro(svcName string, svcVersion string, knownHeaders []string) KMicro {
	km := KMicro{
		svcName:      svcName,
		svcVersion:   svcVersion,
		useOtel:      true,
		knownHeaders: knownHeaders,
		logger:       newLogger(svcName, svcVersion),
	}
	return km
}

func NewKMicroWithoutOtel(svcName string, svcVersion string, knownHeaders []string) KMicro {
	km := KMicro{
		svcName:      svcName,
		svcVersion:   svcVersion,
		useOtel:      false,
		knownHeaders: knownHeaders,
		logger:       newLogger(svcName, svcVersion),
	}
	return km
}

// Connect to nats and setup the micro service
// Use [AddEndpoints] to add endpoints to the service
func (km *KMicro) Start(ctx context.Context, natsUrl string) error {
	if km.useOtel {
		shutdown, tracer, meter, err := setupOTelSDK(ctx, km.svcName)
		if err != nil {
			return fmt.Errorf("could not setup otel %w", err)
		}
		km.otelShutdown = func() error {
			return shutdown(ctx)
		}
		km.tracer = tracer
		km.meter = meter
	} else {
		km.tracer = setupNoopOtel()
		km.otelShutdown = func() error {
			// do nothing
			return nil
		}
	}
	km.logger.Info("connecting to nats...")
	nc, err := nats.Connect(natsUrl)
	if err != nil {
		return err
	}
	km.logger.Info("connected to nats")
	km.nats = nc
	km.natsSvc, err = micro.AddService(nc, micro.Config{
		Name:    km.svcName,
		Version: km.svcVersion,
		DoneHandler: func(srv micro.Service) {
			info := srv.Info()
			km.logger.Info("stopped service", "service", info.Name, "serviceId", info.ID)
		},
		ErrorHandler: func(srv micro.Service, err *micro.NATSError) {
			info := srv.Info()
			km.logger.Info("Service returned an error on subject", "service", info.Name, "subject", err.Subject, "error", err.Description)
		},
	})
	if err != nil {
		return err
	}
	// we need a group to make our endpoints available under svcName.ENDPOINT
	km.Group = km.natsSvc.AddGroup(km.svcName)
	return nil
}

func (km *KMicro) Stop() {
	// we're ignoring all errors
	km.natsSvc.Stop()
	km.nats.Close()
	km.otelShutdown()
}

func (km *KMicro) Logger(module string) *slog.Logger {
	return km.logger.With(slog.String("module", module))
}

func (km *KMicro) AddEndpoint(ctx context.Context, subject string, handler ServiceHandler) {
	km.Group.AddEndpoint(subject, micro.HandlerFunc(func(req micro.Request) {
		ctx := AppendSlogCtx(ctx, slog.String("endpoint", subject))
		go func() {
			start := time.Now()
			propagator := propagation.TraceContext{}
			natsHeaders := req.Headers()
			ctx = propagator.Extract(ctx, propagation.HeaderCarrier(natsHeaders))

			// extract our custom known headers from the nats message
			customHeaders := make(map[string]string, 0)
			for _, k := range km.knownHeaders {
				if val := natsHeaders.Get(k); val != "" {
					customHeaders[k] = val
				}
			}
			ctx = context.WithValue(ctx, CustomCtxHeaders, customHeaders)

			callDepth := 0
			callDepthStr := req.Headers().Get(headerCallDepthKey)
			if callDepthStr != "" {
				val, _ := strconv.Atoi(callDepthStr)
				callDepth = val
			}
			ctx = context.WithValue(ctx, callDepthCtxKey, callDepth)
			ctx, span := km.tracer.Start(ctx, "handle: "+subject)
			defer span.End()

			km.logger.InfoContext(ctx, "handle request")
			result, err := handler(ctx, req.Data())
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				km.logger.ErrorContext(ctx, fmt.Sprintf("handler error (%s): %s", subject, err.Error()))
				req.Error("500", err.Error(), nil)
				return
			}
			req.Respond(result)
			span.SetStatus(codes.Ok, "")
			km.logger.InfoContext(ctx, "handled request", slog.String("duration", time.Since(start).String()))
		}()
	}))
}

// the given ctx should be returned by getContext from kmicro
func (km *KMicro) Call(ctx context.Context, endpoint string, data []byte) ([]byte, error) {
	header := make(nats.Header)

	// prevent infinite loops
	callDepth := 0
	callDepthStr, ok := ctx.Value(callDepthCtxKey).(string)
	if ok {
		val, _ := strconv.Atoi(callDepthStr)
		callDepth = val + 1
	}
	if callDepth > 20 {
		return nil, maxCallDepthErr
	}
	header.Set(headerCallDepthKey, strconv.Itoa(callDepth))
	// add our custom headers
	if currHeaders, ok := ctx.Value(CustomCtxHeaders).(map[string]string); ok {
		for _, k := range km.knownHeaders {
			if val, ok := currHeaders[k]; ok {
				header.Set(k, val)
			}
		}
	}

	// setup tracing
	propagator := propagation.TraceContext{}
	ctx, span := km.tracer.Start(ctx, fmt.Sprintf("call: %s", endpoint))
	ctx = AppendSlogCtx(ctx, slog.String("action", endpoint))
	propagator.Inject(ctx, propagation.HeaderCarrier(header))
	defer span.End()
	// -----
	km.logger.InfoContext(ctx, "call")
	respMsg, err := km.nats.RequestMsgWithContext(ctx, &nats.Msg{
		Subject: endpoint,
		Header:  header,
		Data:    data,
	})
	if err != nil { // this error is from nats and not from a called service
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		km.logger.ErrorContext(ctx, fmt.Sprintf("nats error: %s", err.Error()))
		return nil, err
	}
	isResponseErrorMsg := respMsg.Header.Get("Nats-Service-Error-Code")
	if isResponseErrorMsg != "" {
		errorMsg := respMsg.Header.Get("Nats-Service-Error")
		span.SetStatus(codes.Error, errorMsg)
		span.RecordError(err)
		km.logger.ErrorContext(ctx, fmt.Sprintf("action error: %s", isResponseErrorMsg))
		return nil, fmt.Errorf("action error: %s", errorMsg)
	}
	km.logger.InfoContext(ctx, "received call response")
	span.SetStatus(codes.Ok, "")
	return respMsg.Data, nil
}

func newLogger(svcName, svcVersion string) *slog.Logger {
	initAttr := []slog.Attr{slog.Group("service",
		slog.String("name", svcName),
		slog.String("version", svcVersion),
	)}
	h := &KMicroContextHandler{
		initAttr: initAttr,
		Handler: slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			AddSource: true,
		},
		)}
	return slog.New(h)
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
		r.AddAttrs(slog.String("traceId", spanCtx.TraceID().String()))
	}
	if spanCtx.HasSpanID() {
		r.AddAttrs(slog.String("spanId", spanCtx.SpanID().String()))
	}

	return h.Handler.Handle(ctx, r)
}

func AppendSlogCtx(ctx context.Context, attr slog.Attr) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	if v, ok := ctx.Value(slogFields).([]slog.Attr); ok {
		v = append(v, attr)
		return context.WithValue(ctx, slogFields, v)
	}

	v := []slog.Attr{}
	v = append(v, attr)
	return context.WithValue(ctx, slogFields, v)
}
