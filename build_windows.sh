#!/usr/bin/env bash
# Build the Windows client. Run this on the development machine.
set -euo pipefail

cd "$(dirname "$0")"

echo "Building audio_client.exe (Windows GUI)..."
GOOS=windows GOARCH=amd64 \
  go build -o audio_client.exe -ldflags "-H windowsgui" ./cmd/client

echo "Wrote audio_client.exe"
