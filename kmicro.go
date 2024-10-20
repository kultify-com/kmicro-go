package kmicro

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"go.opentelemetry.io/otel/codes"
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
	knownHeaders []string
}

const (
	callDepthCtxKey = iota
	CustomCtxHeaders
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
	}
	return km
}

func NewKMicroWithoutOtel(svcName string, svcVersion string, knownHeaders []string) KMicro {
	km := KMicro{
		svcName:      svcName,
		svcVersion:   svcVersion,
		useOtel:      false,
		knownHeaders: knownHeaders,
	}
	return km
}

// Connect to nats and setup the micro service
// Use [AddEndpoints] to add endpoints to the service
func (km *KMicro) Start(ctx context.Context, natsUrl string) error {
	if km.useOtel {
		shutdown, tracer, err := setupOTelSDK(ctx, km.svcName)
		if err != nil {
			return fmt.Errorf("could not setup otel %w", err)
		}
		km.otelShutdown = func() error {
			return shutdown(ctx)
		}
		km.tracer = tracer
	} else {
		km.tracer = setupNoopOtel()
		km.otelShutdown = func() error {
			// do nothing
			return nil
		}
	}
	slog.Info("connecting to nats...")
	nc, err := nats.Connect(natsUrl)
	if err != nil {
		return err
	}
	slog.Info("connected to nats")
	km.nats = nc
	km.natsSvc, err = micro.AddService(nc, micro.Config{
		Name:    km.svcName,
		Version: km.svcVersion,
		DoneHandler: func(srv micro.Service) {
			info := srv.Info()
			slog.Info("stopped service", "serivce", info.Name, "serviceId", info.ID)
		},
		ErrorHandler: func(srv micro.Service, err *micro.NATSError) {
			info := srv.Info()
			slog.Info("Service returned an error on subject", "service", info.Name, "subject", err.Subject, "error", err.Description)
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

func (km *KMicro) GetLogger(ctx context.Context, module string) *slog.Logger {
	spanCtx := trace.SpanContextFromContext(ctx)
	otelLogger := slog.Default().With("module", module)
	if spanCtx.HasTraceID() {
		otelLogger = otelLogger.With("traceId", spanCtx.TraceID().String())
	}
	if spanCtx.HasSpanID() {
		otelLogger = otelLogger.With("spanId", spanCtx.SpanID().String())
	}
	return otelLogger
}

func (km *KMicro) AddEndpoint(ctx context.Context, subject string, handler ServiceHandler) {
	km.Group.AddEndpoint(subject, micro.HandlerFunc(func(req micro.Request) {
		go func() {
			propagator := propagation.TraceContext{}
			natsHeaders := req.Headers()
			ctx := propagator.Extract(ctx, propagation.HeaderCarrier(natsHeaders))

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

			result, err := handler(ctx, req.Data())
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				km.GetLogger(ctx, "kmicro").Error(fmt.Sprintf("Handler error (%s): %s", subject, err.Error()))
				req.Error("500", err.Error(), nil)
				return
			}
			req.Respond(result)
			span.SetStatus(codes.Ok, "")
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
	ctx, span := km.tracer.Start(ctx, "call: "+endpoint)
	propagator.Inject(ctx, propagation.HeaderCarrier(header))
	defer span.End()
	// -----
	respMsg, err := km.nats.RequestMsgWithContext(ctx, &nats.Msg{
		Subject: endpoint,
		Header:  header,
		Data:    data,
	})
	if err != nil { // this error is from nats and not from a called service
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		km.GetLogger(ctx, "kmicro").Error(fmt.Sprintf("nats error: %s", err.Error()))
		return nil, err
	}
	isResponseErrorMsg := respMsg.Header.Get("Nats-Service-Error-Code")
	if isResponseErrorMsg != "" {
		errorMsg := respMsg.Header.Get("Nats-Service-Error")
		span.SetStatus(codes.Error, errorMsg)
		span.RecordError(err)
		km.GetLogger(ctx, "kmicro").Error(fmt.Sprintf("action error: %s", isResponseErrorMsg))
		return nil, fmt.Errorf("action error: %s", errorMsg)
	}

	span.SetStatus(codes.Ok, "")
	return respMsg.Data, nil
}
