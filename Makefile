.PHONY: protoc build test run clean setup install

PROTO_SRC = protobuf/service.proto
PROTO_OUT = protobuf/
API_COMMON_PROTOS_DIR = ../api-common-protos
API_COMMON_PROTOS_REPO = https://github.com/googleapis/api-common-protos.git

# Ensure api-common-protos is cloned
setup:
	@if [ ! -d "$(API_COMMON_PROTOS_DIR)" ]; then \
		echo "Cloning api-common-protos..."; \
		git clone $(API_COMMON_PROTOS_REPO) $(API_COMMON_PROTOS_DIR); \
	else \
		echo "api-common-protos already exists."; \
	fi

# Define the protoc command
protoc: setup
	protoc \
		-I . \
		-I $(API_COMMON_PROTOS_DIR) \
		--go_out=$(PROTO_OUT) \
		--go_opt=module=github.com/Bl4ck-h00d/stashdb \
		--go-grpc_out=$(PROTO_OUT) \
		--go-grpc_opt=module=github.com/Bl4ck-h00d/stashdb \
		$(PROTO_SRC)

# Build the Go project
build:
	@echo "Building the Go project..."
	@go build -o bin/server main.go

# Build the CLI tool
build-cli:
	@echo "Building the CLI tool..."
	@go build -o bin/stash main.go

# Install the CLI tool
install: build-cli
	@echo "Installing the CLI tool..."
	@mv bin/stash /usr/local/bin

# Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Run the server
run: build
	@echo "Running the server..."
	@./bin/server start

# Clean build artifacts
clean:
	@echo "Cleaning up..."
	@rm -rf bin
	@find $(PROTO_OUT) -name "*.pb.go" -delete
