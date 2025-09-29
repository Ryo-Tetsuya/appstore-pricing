#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

target="aarch64-apple-darwin"
binary_path="target/$target/release/appstore_pricing"
app_path="target/$target/release/App Store Pricing.app"
zip_path="target/$target/release/appstore-pricing-macos-arm64.zip"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

cargo_path_remap_flags() {
  local sep=$'\x1f'
  local home="${HOME:-}"
  local cargo_home="${CARGO_HOME:-}"
  local rustup_home="${RUSTUP_HOME:-}"
  local flags=(
    "--remap-path-prefix" "$repo_root=."
  )

  if [[ -n "$home" ]]; then
    if [[ -z "$cargo_home" ]]; then
      cargo_home="$home/.cargo"
    fi
    if [[ -z "$rustup_home" ]]; then
      rustup_home="$home/.rustup"
    fi
  fi

  if [[ -n "$cargo_home" ]]; then
    flags+=("--remap-path-prefix" "$cargo_home=/cargo")
  fi
  if [[ -n "$rustup_home" ]]; then
    flags+=("--remap-path-prefix" "$rustup_home=/rustup")
  fi
  if [[ -n "$home" ]]; then
    flags+=("--remap-path-prefix" "$home=~")
  fi

  local IFS="$sep"
  printf '%s' "${flags[*]}"
}

cargo_private() {
  local encoded_rustflags
  encoded_rustflags="$(cargo_path_remap_flags)"
  printf '+ cargo'
  printf ' %q' "$@"
  printf ' [path-remap]\n'
  CARGO_ENCODED_RUSTFLAGS="$encoded_rustflags" cargo "$@"
}

host_os() {
  case "$(uname -s)" in
    Darwin) echo "macos" ;;
    *) echo "unsupported" ;;
  esac
}

host_arch() {
  case "$(uname -m)" in
    arm64 | aarch64) echo "aarch64" ;;
    *) uname -m ;;
  esac
}

run() {
  printf '+'
  printf ' %q' "$@"
  printf '\n'
  "$@"
}

require_macos_arm64() {
  if [[ "$(host_os)" != "macos" ]]; then
    echo "This project only packages a macOS app bundle." >&2
    exit 1
  fi

  if [[ "$(host_arch)" != "aarch64" ]]; then
    echo "This build is intentionally Apple Silicon / ARM64 only." >&2
    exit 1
  fi
}

zip_macos_app() {
  require_command ditto
  rm -f "$zip_path"
  run ditto -c -k --sequesterRsrc --keepParent "$app_path" "$zip_path"
}

build_macos_arm64() {
  require_macos_arm64
  require_command cargo

  cargo_private build --release --locked --target "$target"
  run scripts/package-macos.sh "$binary_path" "$app_path"
  verify_no_local_paths "$app_path/Contents/MacOS/appstore_pricing"
  zip_macos_app

  echo
  echo "Built macOS ARM app:"
  echo "  $app_path"
  echo "  $zip_path"
}

run_checks() {
  require_command cargo
  cargo_private fmt --check
  cargo_private clippy --locked --all-targets -- -D warnings
  cargo_private test --locked
}

verify_no_local_paths() {
  local artifact="$1"
  local pattern='/Users/|/users/|Documents/Git|/private/var/folders|/var/folders'

  require_command strings
  require_command rg
  if strings "$artifact" | LC_ALL=C rg -i "$pattern" >/tmp/appstore-pricing-local-paths.txt; then
    echo "Local build paths are still present in $artifact:" >&2
    sed -n '1,20p' /tmp/appstore-pricing-local-paths.txt >&2
    exit 1
  fi
}

usage() {
  cat <<USAGE
Usage: scripts/build-release.sh [command]

No command builds the macOS ARM .app and zip.

Commands:
  build  Build and package the macOS ARM .app and zip.
  check  Run fmt, clippy, and tests.
  help   Show this help.

Host detected: $(host_os) $(host_arch)
USAGE
}

command_name="${1:-build}"

case "$command_name" in
  build | current | macos) build_macos_arm64 ;;
  check) run_checks ;;
  help | --help | -h) usage ;;
  *)
    echo "Unknown command: $command_name" >&2
    echo >&2
    usage >&2
    exit 1
    ;;
esac
