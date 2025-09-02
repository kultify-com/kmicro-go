package kmicro

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

type KMicro struct {
	micro.Group
	svcName    string
	svcVersion string

	Nats    *nats.Conn
	natsSvc micro.Service

	knownHeaders []string
	logger       *slog.Logger

	// tracing
	tracer                    trace.Tracer
	meter                     metric.Meter
	endpointLatency           metric.Int64Histogram
	endpointProcessedRequests metric.Int64Counter
	endpointFailedRequests    metric.Int64Counter
}

type CtxKey int

const (
	callDepthCtxKey CtxKey = iota
	CustomCtxHeaders
	slogFields
)

type Headers map[string]string

var (
	maxCallDepthErr = errors.New("max call depth reached")
)

const headerCallDepthKey = "kmicro_callDepth"

type ServiceHandler func(ctx context.Context, data []byte) ([]byte, error)

type kmicroOptions struct {
	knownHeaders []string
	logger       *slog.Logger
}

type option func(option *kmicroOptions)

func WithKnownHeaders(knownHeaders []string) func(*kmicroOptions) {
	return func(o *kmicroOptions) {
		o.knownHeaders = knownHeaders
	}
}

func WithLogger(logger *slog.Logger) func(*kmicroOptions) {
	return func(o *kmicroOptions) {
		o.logger = logger
	}
}

func NewKMicro(svcName string, svcVersion string, options ...option) KMicro {
	configuredOptions := kmicroOptions{}
	for _, o := range options {
		o(&configuredOptions)
	}
	usedLogger := configuredOptions.logger
	if usedLogger == nil {
		usedLogger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			AddSource: true,
		}))
	}
	km := KMicro{
		svcName:      svcName,
		svcVersion:   svcVersion,
		knownHeaders: configuredOptions.knownHeaders,
		logger:       setupLogger(usedLogger, svcName, svcVersion),
	}
	return km
}

// Connect to nats and setup the micro service
// Use [AddEndpoints] to add endpoints to the service
func (km *KMicro) Start(ctx context.Context, natsUrl string) error {
	km.tracer = otel.GetTracerProvider().Tracer("kmicro", trace.WithInstrumentationAttributes(
		semconv.ServiceName(km.svcName),
	))
	km.meter = otel.GetMeterProvider().Meter("kmicro", metric.WithInstrumentationAttributes(
		semconv.ServiceName(km.svcName),
	))
	km.logger.Info("connecting to nats...")
	nc, err := nats.Connect(natsUrl)
	if err != nil {
		return err
	}
	km.logger.Info("connected to nats")
	km.Nats = nc
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
		return fmt.Errorf("could not create nats service: %w", err)
	}
	// we need a group to make our endpoints available under svcName.ENDPOINT
	km.Group = km.natsSvc.AddGroup(km.svcName)

	// setup meters
	km.endpointLatency, err = km.meter.Int64Histogram("kmicro.endpoint.latency", metric.WithUnit("ms"))
	if err != nil {
		km.logger.Error(fmt.Sprintf("could not create endpoint.latency histogram %s", err.Error()))
	}
	km.endpointProcessedRequests, err = km.meter.Int64Counter("kmicro.endpoint.requests.success", metric.WithDescription("The number of successfull handled requests"))
	if err != nil {
		km.logger.Error(fmt.Sprintf("could not create endpoint.requests.success histogram %s", err.Error()))
	}
	km.endpointFailedRequests, err = km.meter.Int64Counter("kmicro.endpoint.requests.error", metric.WithDescription("The number of failed requests"))
	if err != nil {
		km.logger.Error(fmt.Sprintf("could not create endpoint.requests.success histogram %s", err.Error()))
	}
	return nil
}

// Stop is used for a clean node shutdown
func (km *KMicro) Stop() {
	if km.natsSvc != nil {
		err := km.natsSvc.Stop()
		if err != nil {
			km.logger.Error(fmt.Sprintf("could not stop nats service %s", err.Error()))
		}
	}
	if km.Nats != nil {
		km.Nats.Close()
	}
}

// Logger returns a slog.Logger with a module label
func (km *KMicro) Logger(module string) *slog.Logger {
	return km.logger.With(slog.String("module", module))
}

// AddEndpoint registers a new endpoint to handle incoming requests
func (km *KMicro) AddEndpoint(ctx context.Context, subject string, handler ServiceHandler) error {
	ctx = AppendSlogCtx(ctx, slog.String("endpoint", subject))
	metricAttrs := metric.WithAttributes(
		semconv.RPCMethod(subject),
	)
	err := km.Group.AddEndpoint(subject, micro.HandlerFunc(func(req micro.Request) {
		start := time.Now()
		propagator := propagation.TraceContext{}
		natsHeaders := req.Headers()
		ctx = propagator.Extract(ctx, propagation.HeaderCarrier(natsHeaders))

		// extract our custom known headers from the nats message
		customHeaders := make(Headers, len(km.knownHeaders))
		for _, k := range km.knownHeaders {
			if val := natsHeaders.Get(k); val != "" {
				customHeaders[k] = val
			}
		}
		ctx = ContextWithCustomHeaders(ctx, customHeaders)

		callDepth := 0
		callDepthStr := req.Headers().Get(headerCallDepthKey)
		if callDepthStr != "" {
			val, _ := strconv.Atoi(callDepthStr)
			callDepth = val
		}
		ctx = context.WithValue(ctx, callDepthCtxKey, callDepth)
		ctx, span := km.tracer.Start(ctx, fmt.Sprintf("handle: %s", subject))
		defer span.End()
		km.logger.InfoContext(ctx, "handle request")
		result, err := handler(ctx, req.Data())
		duration := time.Since(start)
		km.endpointLatency.Record(ctx, duration.Milliseconds(), metricAttrs)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			km.logger.ErrorContext(ctx, fmt.Sprintf("handler error (%s): %s", subject, err.Error()))
			req.Error("500", err.Error(), nil)
			km.endpointFailedRequests.Add(ctx, 1, metricAttrs)
			return
		}
		err = req.Respond(result)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			km.logger.ErrorContext(ctx, fmt.Sprintf("could not respond to request (%s): %s", subject, err.Error()))
			km.endpointFailedRequests.Add(ctx, 1, metricAttrs)
			return
		}
		span.SetStatus(codes.Ok, "")
		km.endpointProcessedRequests.Add(ctx, 1, metricAttrs)
		km.logger.InfoContext(ctx, "handled request", slog.String("duration", time.Since(start).String()))
	}))
	return err
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
	if currHeaders, ok := ctx.Value(CustomCtxHeaders).(Headers); ok {
		for _, k := range km.knownHeaders {
			if val, ok := currHeaders[k]; ok {
				header.Set(k, val)
			}
		}
	}

	// setup tracing
	propagator := propagation.TraceContext{}
	parts := strings.Split(endpoint, ".")
	rpcService := parts[0]
	rpcAction := parts[1]
	ctx, span := km.tracer.Start(ctx, fmt.Sprintf("call: %s", endpoint),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(semconv.RPCService(rpcService),
			semconv.RPCMethod(rpcAction)),
	)
	propagator.Inject(ctx, propagation.HeaderCarrier(header))
	defer span.End()
	// -----
	km.logger.InfoContext(ctx, "call", slog.String("endpoint", endpoint))
	respMsg, err := km.Nats.RequestMsgWithContext(ctx, &nats.Msg{
		Subject: endpoint,
		Header:  header,
		Data:    data,
	})
	if err != nil { // this error is from nats and not from a called service
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		km.logger.ErrorContext(ctx, fmt.Sprintf("nats error (%s): %s", endpoint, err.Error()))
		return nil, err
	}
	isResponseErrorMsg := respMsg.Header.Get("Nats-Service-Error-Code")
	if isResponseErrorMsg != "" {
		errorMsg := respMsg.Header.Get("Nats-Service-Error")
		span.SetStatus(codes.Error, errorMsg)
		span.RecordError(err)
		km.logger.ErrorContext(ctx, fmt.Sprintf("action error (%s): %s", endpoint, isResponseErrorMsg))
		return nil, fmt.Errorf("action error: %s", errorMsg)
	}
	km.logger.InfoContext(ctx, "received call response", slog.String("endpoint", endpoint))
	span.SetStatus(codes.Ok, "")
	return respMsg.Data, nil
}

// AppendSlogCtx returns a context with the given attr
func AppendSlogCtx(ctx context.Context, attrs ...slog.Attr) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	if v, ok := ctx.Value(slogFields).([]slog.Attr); ok {
		v = append(v, attrs...)
		return context.WithValue(ctx, slogFields, v)
	}

	v := []slog.Attr{}
	v = append(v, attrs...)
	return context.WithValue(ctx, slogFields, v)
}

func ContextWithCustomHeaders(ctx context.Context, headers Headers) context.Context {
	return context.WithValue(ctx, CustomCtxHeaders, headers)
}

func CustomHeadersFromContext(ctx context.Context) (Headers, bool) {
	headers, ok := ctx.Value(CustomCtxHeaders).(Headers)
	return headers, ok
}
