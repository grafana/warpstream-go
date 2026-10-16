#!/usr/bin/env bash
set -euo pipefail

module_dir="$(cd "$(dirname "$0")" && pwd)"
repo_dir="$(git -C "$module_dir" rev-parse --show-toplevel)"
build_dir="${1:-$module_dir/.build}"
mkdir -p "$build_dir"
build_dir="$(cd "$build_dir" && pwd)"
version="$(cat "$module_dir/mimir-version.txt")"
cd "$module_dir"
go mod download "github.com/grafana/mimir@$version"
source_dir="$(go list -m -f '{{.Dir}}' "github.com/grafana/mimir@$version")"
cp "$source_dir/go.mod" "$build_dir/go.mod"
cp "$source_dir/go.sum" "$build_dir/go.sum"
chmod u+w "$build_dir/go.mod" "$build_dir/go.sum"
if [[ -n "${WARPSTREAM_CLIENT_VERSION:-}" ]]; then
  go mod edit -modfile="$build_dir/go.mod" -replace="github.com/grafana/warpstream-go=github.com/grafana/warpstream-go@$WARPSTREAM_CLIENT_VERSION"
else
  go mod edit -modfile="$build_dir/go.mod" -replace="github.com/grafana/warpstream-go=$repo_dir"
fi
(
  cd "$source_dir"
  GOMAXPROCS="${GOMAXPROCS:-4}" go build -p "${GOMAXPROCS:-4}" -mod=mod \
    -modfile="$build_dir/go.mod" -o "$build_dir/mimir" ./cmd/mimir
)
go version -m "$build_dir/mimir" > "$build_dir/binary-modules.txt"
