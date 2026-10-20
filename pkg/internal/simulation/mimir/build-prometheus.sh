#!/usr/bin/env bash
set -euo pipefail

module_dir="$(cd "$(dirname "$0")" && pwd)"
build_dir="${1:-$module_dir/.build}"
mkdir -p "$build_dir"
build_dir="$(cd "$build_dir" && pwd)"
version="$(cat "$module_dir/prometheus-version.txt")"
cd "$module_dir"
source_dir="$(go mod download -json "github.com/prometheus/prometheus@$version" | python3 -c 'import json,sys; print(json.load(sys.stdin)["Dir"])')"
(
  cd "$source_dir"
  GOWORK=off GOMAXPROCS="${GOMAXPROCS:-4}" go build -p "${GOMAXPROCS:-4}" -mod=readonly \
    -o "$build_dir/prometheus" ./cmd/prometheus
)
go version -m "$build_dir/prometheus" > "$build_dir/prometheus-modules.txt"
