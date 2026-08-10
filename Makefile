# Variables
GO := go
GOTEST := $(GO) test
GOMOD := $(GO) mod
GOCLEAN := $(GO) clean


# Directories
SRC_DIR := ./

# Targets

.PHONY: test clean run

# Run tests
# -race is required: the endpoint handler races are only observable under the
# detector, and consumers run their suites with it.
test:
	@echo "Running tests..."
	$(GOTEST) -race ./...

# Update dependencies
tidy:
	@echo "Tidying up dependencies..."
	$(GOMOD) tidy
