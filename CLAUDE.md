# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Testing
- `make test` - Run all tests
- `go test ./...` - Run tests directly

### Dependencies
- `make tidy` - Update and clean Go module dependencies
- `go mod tidy` - Update dependencies directly

## Code Architecture

### Core Framework (kmicro.go)
This is a Go microservice framework built on top of NATS Micro that provides:
- **KMicro struct**: Main service wrapper that handles NATS connection, service registration, and telemetry
- **Service lifecycle**: Start/Stop methods for clean service management
- **Endpoint registration**: AddEndpoint method to register service handlers
- **Inter-service communication**: Call method for service-to-service communication with automatic tracing
- **Request context**: Custom context handling with headers, call depth tracking, and slog integration
- **Telemetry**: Built-in OpenTelemetry metrics and tracing for all endpoints

### Transfer Service (transfer/)
Object storage abstraction layer using NATS JetStream:
- **TransferService**: Main service for object storage operations
- **TransferReference**: Object identification (bucket + key)
- **Operations**: CreateBucket, DeleteBucket, Write, Read, Delete
- **Tracking**: Optional operation tracking with TrackLog for debugging

### OpenTelemetry Configuration (otel/)
Centralized telemetry setup:
- **ConfigureOpenTelemetry**: Bootstrap function for traces and metrics
- **OTLP exporters**: Configured for gRPC with insecure connections
- **Resource attributes**: Service name and version tagging

### Key Patterns
- **Context propagation**: Automatic trace context passing between services
- **Header management**: Known headers are automatically forwarded in service calls
- **Call depth protection**: Prevents infinite loops in service-to-service calls (max 20 depth)
- **Structured logging**: slog integration with contextual fields
- **Error handling**: Consistent error wrapping and telemetry recording

### Service Communication
Services communicate via NATS subjects in the format `{serviceName}.{endpoint}`. The framework automatically handles:
- Trace context propagation
- Custom header forwarding
- Call depth tracking
- Metrics collection for latency and request counts