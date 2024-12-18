# kmicro-go
Simple microservice framework for go based on nats micro


## OTEL Sample Setup for Services

```
func setupOTelSDK(ctx context.Context, svcName string) (shutdown func(context.Context) error, tracer trace.Tracer, meter metric.Meter, err error) {
	var shutdownFuncs []func(context.Context) error

	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	// traces
	trProvider, err := newTracerProvider(ctx, svcName)
	if err != nil {
		handleErr(err)
		return
	}
	tracer = trProvider.Tracer(svcName)
	shutdownFuncs = append(shutdownFuncs, trProvider.Shutdown)

	// metrics
	meterProvider, err := newMeterProvider(ctx, svcName)
	if err != nil {
		handleErr(err)
		return
	}
	meter = meterProvider.Meter(svcName)
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	return
}

func setupNoopOtel() (tracer trace.Tracer, meter metric.Meter) {
	tracer = traceNoop.NewTracerProvider().Tracer("")
	meter = metricNoop.Meter{}
	return
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newTracerProvider(ctx context.Context, svcName string) (*sdkTrace.TracerProvider, error) {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(svcName),
		),
	)

	// with insecure is needed to enable passing logs to https grpcs endpoints
	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	traceProvider := sdkTrace.NewTracerProvider(
		sdkTrace.WithBatcher(traceExporter),
		sdkTrace.WithResource(res),
	)
	return traceProvider, nil
}

func newMeterProvider(ctx context.Context, svcName string) (*sdkMetric.MeterProvider, error) {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(svcName),
		),
	)
	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithInsecure(), otlpmetricgrpc.WithCompressor("gzip"))
	if err != nil {
		return nil, err
	}

	meterProvider := sdkMetric.NewMeterProvider(
		sdkMetric.WithResource(res),
		sdkMetric.WithReader(sdkMetric.NewPeriodicReader(metricExporter,
			// 1 minute is also the default
			sdkMetric.WithInterval(1*time.Minute))),
	)
	return meterProvider, nil
}
```
