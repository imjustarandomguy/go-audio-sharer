#!/bin/sh
# Add user go bin to path (assuming default GOPATH location or readable via go env)
# We use export for sh/bash compatibility
export PATH=$PATH:$(go env GOPATH)/bin

# Check if protoc is available
if ! command -v protoc >/dev/null 2>&1; then
    echo "protoc not found in PATH"
    exit 1
fi

protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/audio.proto
