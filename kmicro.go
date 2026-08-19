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
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

type KMicro struct {
	svcName    string
	svcVersion string

	Nats               *nats.Conn
	natsSvc            micro.Service
	isExternalNatsConn bool

	js jetstream.JetStream

	knownHeaders       []string
	eventSubjectPrefix string
	logger             *slog.Logger
	eventConsumers     []jetstream.ConsumeContext
	stopTimeout        time.Duration

	// tracing
	tracer                    trace.Tracer
	meter                     metric.Meter
	endpointLatency           metric.Int64Histogram
	endpointProcessedRequests metric.Int64Counter
	endpointFailedRequests    metric.Int64Counter
	eventPublished            metric.Int64Counter
	eventProcessed            metric.Int64Counter
	eventFailed               metric.Int64Counter
	eventLatency              metric.Int64Histogram
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

// headerDeadlineKey carries the caller's context deadline (unix nanos) across the
// NATS request so the handler goroutine can bound its own ctx to what's left of it,
// instead of running unbounded after the caller has already given up.
const headerDeadlineKey = "kmicro_deadline_unix_nano"

type ServiceHandler func(ctx context.Context, data []byte) ([]byte, error)

// EndpointInterceptor runs before a handler; a non-nil error rejects the
// request with a 403 service error. Use it to scope who may call an endpoint
// (e.g. only platform instances may invoke "<module>.__http").
type EndpointInterceptor func(ctx context.Context, data []byte) error

type endpointConfig struct{ interceptors []EndpointInterceptor }

// EndpointOption configures an endpoint at registration time, e.g. attaching
// interceptors via [WithInterceptor].
type EndpointOption func(*endpointConfig)

// WithInterceptor registers an [EndpointInterceptor] that runs before the
// endpoint's handler. Multiple interceptors run in the order they were
// added; the first non-nil error rejects the request.
func WithInterceptor(i EndpointInterceptor) EndpointOption {
	return func(c *endpointConfig) { c.interceptors = append(c.interceptors, i) }
}

type kmicroOptions struct {
	knownHeaders       []string
	eventSubjectPrefix string
	logger             *slog.Logger
	stopTimeout        time.Duration
}

// The wait is a client-side teardown and never includes handler work.
const defaultStopTimeout = 5 * time.Second

// ErrConsumersStillSubscribed reports that a node was left subscribed: messages
// it receives now are discarded unacked and redeliver after the consumer
// AckWait.
var ErrConsumersStillSubscribed = errors.New("event consumers did not finish unsubscribing")

type option func(option *kmicroOptions)

func WithStopTimeout(timeout time.Duration) func(*kmicroOptions) {
	return func(o *kmicroOptions) {
		o.stopTimeout = timeout
	}
}

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

func WithEventSubjectPrefix(prefix string) func(*kmicroOptions) {
	return func(o *kmicroOptions) {
		o.eventSubjectPrefix = prefix
	}
}

// NewKMicro creates a new kmicro instance
// svcName is the name of the nats service. Sub-services can be added using [AddGroup]
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
	stopTimeout := configuredOptions.stopTimeout
	if stopTimeout <= 0 {
		stopTimeout = defaultStopTimeout
	}
	km := KMicro{
		svcName:            svcName,
		svcVersion:         svcVersion,
		knownHeaders:       configuredOptions.knownHeaders,
		eventSubjectPrefix: configuredOptions.eventSubjectPrefix,
		logger:             setupLogger(usedLogger, svcName, svcVersion),
		stopTimeout:        stopTimeout,
	}
	return km
}

type StartOption func(*startOptions)

type startOptions struct {
	natsURL  string
	natsConn *nats.Conn
}

func WithNatsURL(url string) StartOption {
	return func(o *startOptions) {
		o.natsURL = url
	}
}

func WithNatsConnection(conn *nats.Conn) StartOption {
	return func(o *startOptions) {
		o.natsConn = conn
	}
}

// Connect to nats and setup the micro service
// Use [AddEndpoints] to add endpoints to the service
func (km *KMicro) Start(ctx context.Context, options ...StartOption) error {
	startOpts := startOptions{
		natsURL: "",
	}
	for _, o := range options {
		o(&startOpts)
	}

	if startOpts.natsURL != "" && startOpts.natsConn != nil {
		return errors.New("cannot use both nats url and nats connection")
	}
	if startOpts.natsConn == nil && startOpts.natsURL == "" {
		return errors.New("either nats url or nats connection must be provided")
	}

	km.tracer = otel.GetTracerProvider().Tracer("kmicro", trace.WithInstrumentationAttributes(
		semconv.ServiceName(km.svcName),
	))
	km.meter = otel.GetMeterProvider().Meter("kmicro", metric.WithInstrumentationAttributes(
		semconv.ServiceName(km.svcName),
	))

	if startOpts.natsConn != nil {
		km.Nats = startOpts.natsConn
		km.isExternalNatsConn = true
	}
	if startOpts.natsURL != "" {
		km.logger.Info("connecting to nats...")
		nc, err := nats.Connect(startOpts.natsURL)
		if err != nil {
			return err
		}
		km.logger.Info("connected to nats")
		km.Nats = nc
	}
	natsSvc, err := micro.AddService(km.Nats, micro.Config{
		Name:    km.svcName,
		Version: km.svcVersion,
		DoneHandler: func(srv micro.Service) {
			info := srv.Info()
			km.logger.Info("stopped service", "service", info.Name, "serviceId", info.ID)
		},
		ErrorHandler: func(srv micro.Service, err *micro.NATSError) {
			info := srv.Info()
			km.logger.Error("Service returned an error on subject", "service", info.Name, "subject", err.Subject, "error", err.Description)
		},
	})
	if err != nil {
		return fmt.Errorf("could not create nats service: %w", err)
	}
	km.natsSvc = natsSvc

	// setup meters
	km.endpointLatency, err = km.meter.Int64Histogram("kmicro.endpoint.latency", metric.WithUnit("ms"))
	if err != nil {
		km.logger.Error(fmt.Sprintf("could not create endpoint.latency histogram %s", err.Error()))
	}
	km.endpointProcessedRequests, err = km.meter.Int64Counter("kmicro.endpoint.requests.success", metric.WithDescription("The number of successful handled requests"))
	if err != nil {
		km.logger.Error(fmt.Sprintf("could not create endpoint.requests.success histogram %s", err.Error()))
	}
	km.endpointFailedRequests, err = km.meter.Int64Counter("kmicro.endpoint.requests.error", metric.WithDescription("The number of failed requests"))
	if err != nil {
		km.logger.Error(fmt.Sprintf("could not create endpoint.requests.success histogram %s", err.Error()))
	}
	km.eventPublished, err = km.meter.Int64Counter("kmicro.events.published", metric.WithDescription("The number of published events"))
	if err != nil {
		km.logger.Error(fmt.Sprintf("could not create events.published counter %s", err.Error()))
	}
	km.eventProcessed, err = km.meter.Int64Counter("kmicro.events.processed", metric.WithDescription("The number of successfully processed events"))
	if err != nil {
		km.logger.Error(fmt.Sprintf("could not create events.processed counter %s", err.Error()))
	}
	km.eventFailed, err = km.meter.Int64Counter("kmicro.events.failed", metric.WithDescription("The number of failed event processing attempts"))
	if err != nil {
		km.logger.Error(fmt.Sprintf("could not create events.failed counter %s", err.Error()))
	}
	km.eventLatency, err = km.meter.Int64Histogram("kmicro.events.latency", metric.WithUnit("ms"))
	if err != nil {
		km.logger.Error(fmt.Sprintf("could not create events.latency histogram %s", err.Error()))
	}
	return nil
}

type Group struct {
	micro.Group
	Name string
}

func (km *KMicro) AddGroup(name string) *Group {
	g := km.natsSvc.AddGroup(name)
	return &Group{
		Group: g,
		Name:  name,
	}
}

func (km *KMicro) JetStream() (jetstream.JetStream, error) {
	if km.js != nil {
		return km.js, nil
	}
	js, err := jetstream.New(km.Nats)
	if err != nil {
		return nil, fmt.Errorf("could not create jetstream context: %w", err)
	}
	km.js = js
	return js, nil
}

// Stop is used for a clean node shutdown. It returns once every event consumer
// has finished unsubscribing, or after the configured stop timeout.
func (km *KMicro) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), km.stopTimeout)
	defer cancel()
	if err := km.StopContext(ctx); err != nil {
		km.logger.Error(err.Error())
	}
}

// StopContext is Stop bounded by the caller's context. It reports
// [ErrConsumersStillSubscribed] when the context ended before every consumer
// had unsubscribed.
func (km *KMicro) StopContext(ctx context.Context) error {
	err := km.stopEventConsumers(ctx)
	if km.natsSvc != nil {
		if stopErr := km.natsSvc.Stop(); stopErr != nil {
			km.logger.Error(fmt.Sprintf("could not stop nats service %s", stopErr.Error()))
		}
	}
	if km.Nats != nil && !km.isExternalNatsConn {
		km.Nats.Close()
	}
	return err
}

func (km *KMicro) stopEventConsumers(ctx context.Context) error {
	closed := make([]<-chan struct{}, 0, len(km.eventConsumers))
	for _, c := range km.eventConsumers {
		closed = append(closed, c.Closed())
		c.Stop()
	}
	for i, ch := range closed {
		select {
		case <-ch:
		case <-ctx.Done():
			return fmt.Errorf("%w: %d of %d remaining: %w",
				ErrConsumersStillSubscribed, len(closed)-i, len(closed), ctx.Err())
		}
	}
	return nil
}

// Logger returns a slog.Logger with a module label
func (km *KMicro) Logger(module string) *slog.Logger {
	return km.logger.With(slog.String("module", module))
}

// AddEndpoint registers a new endpoint to handle incoming requests. Pass
// [WithInterceptor] options to scope which callers may invoke the endpoint.
func (km *KMicro) AddEndpoint(ctx context.Context, group *Group, subject string, handler ServiceHandler, opts ...EndpointOption) error {
	cfg := endpointConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	ctx = AppendSlogCtx(ctx, slog.String("endpoint", subject), slog.String("group", group.Name))
	metricAttrs := metric.WithAttributes(
		semconv.RPCMethod(fmt.Sprintf("%s.%s", group.Name, subject)),
	)
	err := group.AddEndpoint(subject, micro.HandlerFunc(func(req micro.Request) {
		// Read everything off the Request before returning, and answer the caller
		// through `reply` rather than the Request itself. micro's reqHandler reads
		// req.respondError to update endpoint stats the moment this returns, so a
		// Request touched from the goroutine below is a data race.
		natsHeaders := req.Headers()
		reqData := req.Data()
		reply := req.Reply()
		// we need to wrap our handler code because nats has to return as fast a possible
		// to acknowledge the message
		go func() {
			start := time.Now()
			propagator := propagation.TraceContext{}
			// Derive a request-scoped context; never reassign the closure-captured
			// `ctx` (the registration-time parameter shared across every request
			// goroutine). A per-request deadline+cancel below would otherwise cancel
			// the shared ctx when this request returns, canceling later requests.
			reqCtx := propagator.Extract(ctx, propagation.HeaderCarrier(natsHeaders))
			// extract our custom known headers from the nats message
			customHeaders := make(Headers, len(km.knownHeaders))
			for _, k := range km.knownHeaders {
				if val := natsHeaders.Get(k); val != "" {
					customHeaders[k] = val
				}
			}
			reqCtx = ContextWithCustomHeaders(reqCtx, customHeaders)

			callDepth := 0
			callDepthStr := natsHeaders.Get(headerCallDepthKey)
			if callDepthStr != "" {
				val, _ := strconv.Atoi(callDepthStr)
				callDepth = val
			}
			reqCtx = context.WithValue(reqCtx, callDepthCtxKey, callDepth)

			// bound the handler ctx to whatever's left of the caller's deadline, so a
			// caller that has already timed out doesn't leave the handler running on.
			if dl := natsHeaders.Get(headerDeadlineKey); dl != "" {
				if ns, perr := strconv.ParseInt(dl, 10, 64); perr == nil {
					var cancel context.CancelFunc
					reqCtx, cancel = context.WithDeadline(reqCtx, time.Unix(0, ns))
					defer cancel()
				}
			}

			reqCtx, span := km.tracer.Start(reqCtx, fmt.Sprintf("handle: %s", subject))
			defer span.End()
			km.logger.InfoContext(reqCtx, "handle request")
			for _, ic := range cfg.interceptors {
				if ierr := ic(reqCtx, reqData); ierr != nil {
					span.RecordError(ierr)
					span.SetStatus(codes.Error, ierr.Error())
					km.replyError(reply, replyCode(ierr, ErrorCodeForbidden), ierr.Error())
					return
				}
			}
			result, err := handler(reqCtx, reqData)
			duration := time.Since(start)
			km.endpointLatency.Record(reqCtx, duration.Milliseconds(), metricAttrs)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				km.logger.ErrorContext(reqCtx, fmt.Sprintf("handler error (%s): %s", subject, err.Error()))
				km.replyError(reply, replyCode(err, ErrorCodeInternal), err.Error())
				km.endpointFailedRequests.Add(reqCtx, 1, metricAttrs)
				return
			}
			err = km.reply(reply, result)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				km.logger.ErrorContext(reqCtx, fmt.Sprintf("could not respond to request (%s): %s", subject, err.Error()))
				km.endpointFailedRequests.Add(reqCtx, 1, metricAttrs)
				return
			}
			span.SetStatus(codes.Ok, "")
			km.endpointProcessedRequests.Add(reqCtx, 1, metricAttrs)
			km.logger.InfoContext(reqCtx, "handled request", slog.String("group", group.Name), slog.String("duration", time.Since(start).String()))
		}()
	}))
	return err
}

func (km *KMicro) reply(subject string, data []byte) error {
	if subject == "" {
		return nil
	}
	return km.Nats.PublishMsg(&nats.Msg{Subject: subject, Data: data})
}

func (km *KMicro) replyError(subject string, code string, description string) error {
	if subject == "" {
		return nil
	}
	return km.Nats.PublishMsg(&nats.Msg{
		Subject: subject,
		Header: nats.Header{
			micro.ErrorHeader:     []string{description},
			micro.ErrorCodeHeader: []string{code},
		},
	})
}

func (km *KMicro) Call(ctx context.Context, endpoint string, data []byte) ([]byte, error) {
	if _, ok := ctx.Deadline(); !ok {
		km.logger.WarnContext(ctx, "Call invoked without context deadline", slog.String("endpoint", endpoint))
	}

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
	if deadline, ok := ctx.Deadline(); ok {
		header.Set(headerDeadlineKey, strconv.FormatInt(deadline.UnixNano(), 10))
	}
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
	isResponseErrorMsg := respMsg.Header.Get(micro.ErrorCodeHeader)
	if isResponseErrorMsg != "" {
		errorMsg := respMsg.Header.Get(micro.ErrorHeader)
		callErr := &CallError{Code: isResponseErrorMsg, Wrapped: fmt.Errorf("action error: %s", errorMsg)}
		span.SetStatus(codes.Error, errorMsg)
		span.RecordError(callErr)
		km.logger.ErrorContext(ctx, fmt.Sprintf("action error (%s): %s", endpoint, isResponseErrorMsg))
		return nil, callErr
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
