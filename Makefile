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
test:
	@echo "Running tests..."
	$(GOTEST) ./...

# Update dependencies
tidy:
	@echo "Tidying up dependencies..."
	$(GOMOD) tidy
