#!/usr/bin/env bash
set -euo pipefail

module_dir="$(cd "$(dirname "$0")" && pwd)"
build_dir="${1:-$module_dir/.build}"
mkdir -p "$build_dir"
build_dir="$(cd "$build_dir" && pwd)"
version="$(cat "$module_dir/prometheus-version.txt")"
case "$(uname -s)" in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) echo "Unsupported Prometheus platform: $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64) arch=amd64 ;;
  arm64|aarch64) arch=arm64 ;;
  *) echo "Unsupported Prometheus architecture: $(uname -m)" >&2; exit 1 ;;
esac
package="prometheus-${version#v}.$os-$arch"
archive="$package.tar.gz"
checksum="$(awk -v archive="$archive" '$2 == archive {print $1}' "$module_dir/prometheus-sha256sums.txt")"
if [[ -z "$checksum" ]]; then
  echo "Missing pinned checksum for $archive" >&2
  exit 1
fi
download_dir="$(mktemp -d "$build_dir/.prometheus-download.XXXXXX")"
trap 'rm -rf "$download_dir"' EXIT
curl --fail --location --retry 3 --silent --show-error \
  "https://github.com/prometheus/prometheus/releases/download/$version/$archive" \
  -o "$download_dir/$archive"
(
  cd "$download_dir"
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s  %s\n' "$checksum" "$archive" | sha256sum --check -
  else
    printf '%s  %s\n' "$checksum" "$archive" | shasum -a 256 --check -
  fi
)
tar -xzf "$download_dir/$archive" -C "$download_dir" "$package/prometheus"
install -m 755 "$download_dir/$package/prometheus" "$build_dir/prometheus"
go version -m "$build_dir/prometheus" > "$build_dir/prometheus-modules.txt"
