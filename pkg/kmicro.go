package kmicro

import (
	"context"
	"fmt"
	"log"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
)

type KMicro struct {
	micro.Group
	svcName        string
	svcVersion     string
	con            *nats.Conn
	svc            micro.Service
	logger         slog.Logger
	otelShutdown   func() error
	tracerProvider *trace.TracerProvider
}

const (
	natsCon = iota
	traceProviderKey
)

type ServiceHandler func(ctx context.Context, data []byte) ([]byte, error)

func NewKMicro(svcName string, svcVersion string) KMicro {
	km := KMicro{
		svcName:    svcName,
		svcVersion: svcVersion,
	}
	return km
}

// Connect to nats and setup the micro service
// Use [AddEndpoints] to add endpoints to the service
func (km *KMicro) Start(ctx context.Context, natsUrl string) error {
	shutdown, tracerProvider, err := setupOTelSDK(ctx, km.svcName)
	if err != nil {
		return fmt.Errorf("could not setup otel %w", err)
	}
	km.otelShutdown = func() error {
		return shutdown(ctx)
	}
	km.tracerProvider = tracerProvider

	nc, err := nats.Connect(natsUrl)
	if err != nil {
		return err
	}
	km.con = nc
	km.svc, err = micro.AddService(nc, micro.Config{
		Name:    km.svcName,
		Version: km.svcVersion,
		DoneHandler: func(srv micro.Service) {
			info := srv.Info()
			fmt.Printf("stopped service %q with ID %q\n", info.Name, info.ID)
		},
		ErrorHandler: func(srv micro.Service, err *micro.NATSError) {
			info := srv.Info()
			fmt.Printf("Service %q returned an error on subject %q: %s", info.Name, err.Subject, err.Description)
		},
	})
	// we need a group to make our endpoints available under svcName.ENDPOINT
	km.Group = km.svc.AddGroup(km.svcName)
	if err != nil {
		return err
	}
	return nil
}

func (km *KMicro) Stop() {
	// we're ignoring all errors
	km.svc.Stop()
	km.con.Close()
	km.otelShutdown()
}

func (km *KMicro) AddEndpoint(subject string, handler ServiceHandler) {
	// wrap everything to add tracing to all incoming requests
	log.Printf("add endpoint to: %s", subject)
	km.Group.AddEndpoint(subject, micro.HandlerFunc(func(req micro.Request) {
		go func() {
			propagator := propagation.TraceContext{}
			ctx := km.GetContext(context.Background()) // could be improved with timeouts etc.
			ctx = propagator.Extract(ctx, propagation.HeaderCarrier(req.Headers()))
			tracer := km.tracerProvider.Tracer("")
			ctx, span := tracer.Start(ctx, "handle: "+subject)
			defer span.End()

			result, err := handler(ctx, req.Data())
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				req.Error("500", err.Error(), nil)
			}
			req.Respond(result)
			span.SetStatus(codes.Ok, "")
		}()
	}))
}

func (km *KMicro) GetContext(ctx context.Context) context.Context {
	newCtx := context.WithValue(ctx, natsCon, km.con)
	return context.WithValue(newCtx, traceProviderKey, km.tracerProvider)
}

// the given ctx should be returned by getContext from kmicro
func DoCall(ctx context.Context, endpoint string, data []byte) ([]byte, error) {
	nc, ok := ctx.Value(natsCon).(*nats.Conn)
	if ok == false {
		return nil, fmt.Errorf("nats connection not in context")
	}
	traceProvider, ok := ctx.Value(traceProviderKey).(*trace.TracerProvider)
	if ok == false {
		return nil, fmt.Errorf("service name not in context")
	}

	header := make(nats.Header)
	// tracing
	propagator := propagation.TraceContext{}
	tracer := traceProvider.Tracer("")
	ctx, span := tracer.Start(ctx, "call: "+endpoint)
	propagator.Inject(ctx, propagation.HeaderCarrier(header))
	defer span.End()
	// -----
	respMsg, err := nc.RequestMsgWithContext(ctx, &nats.Msg{
		Subject: endpoint,
		Header:  header,
		Data:    data,
	})
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return nil, err
	}
	span.SetStatus(codes.Ok, "")
	return respMsg.Data, nil
}
