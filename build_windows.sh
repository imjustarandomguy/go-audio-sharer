#!/usr/bin/env bash
# Build the Windows client.
#
# NOTE: The audio backend (malgo/miniaudio) requires cgo, so this cannot be
# cross-compiled as a pure-Go build from Linux. You have two options:
#
#   A) Build ON the Windows machine (recommended):
#        * Install Go for Windows
#        * From this repo on Windows, run:
#            go build -o audio_client.exe -ldflags "-H windowsgui" ./cmd/client
#
#   B) Cross-compile from Linux with a mingw-w64 C toolchain:
#        sudo apt install gcc-mingw-w64-x86-64
#        CGO_ENABLED=1 GOOS=windows CC=x86_64-w64-mingw32-gcc \
#          go build -o audio_client.exe -ldflags "-H windowsgui" ./cmd/client
set -euo pipefail
cd "$(dirname "$0")"

if command -v x86_64-w64-mingw32-gcc >/dev/null 2>&1; then
  echo "Building audio_client.exe (Windows GUI) with mingw-w64 cross compiler..."
  CGO_ENABLED=1 GOOS=windows CC=x86_64-w64-mingw32-gcc \
    go build -o audio_client.exe -ldflags "-H windowsgui" ./cmd/client
  echo "Wrote audio_client.exe"
else
  echo
  echo "No mingw-w64 cross compiler found (x86_64-w64-mingw32-gcc)."
  echo "Build the Windows client ON the Windows machine, or install the cross"
  echo "toolchain, e.g.:  sudo apt install gcc-mingw-w64-x86-64"
  echo
  echo "Windows-machine command:"
  echo "  go build -o audio_client.exe -ldflags \"-H windowsgui\" ./cmd/client"
  exit 1
fi
