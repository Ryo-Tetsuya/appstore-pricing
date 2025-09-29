#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

binary_path="${1:-target/aarch64-apple-darwin/release/appstore_pricing}"
app_path="${2:-target/aarch64-apple-darwin/release/App Store Pricing.app}"
bundle_name="${APPSTORE_PRICING_BUNDLE_NAME:-App Store Pricing}"
bundle_id="${APPSTORE_PRICING_BUNDLE_ID:-dev.local.appstore-pricing}"
version="${APPSTORE_PRICING_BUNDLE_VERSION:-0.1.0}"
executable_name="appstore_pricing"
iconset_path="target/macos/AppIcon.iconset"
icon_name="AppIcon.icns"
swift_cache_path="target/macos/swift-module-cache"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This package script only builds macOS .app bundles." >&2
  exit 1
fi

if [[ "$(uname -m)" != "arm64" ]]; then
  echo "This package script is intentionally ARM-only. Run it on Apple Silicon." >&2
  exit 1
fi

if [[ ! -f "$binary_path" ]]; then
  echo "Binary not found: $binary_path" >&2
  exit 1
fi

require_command swift

rm -rf "$app_path"
mkdir -p "$app_path/Contents/MacOS" "$app_path/Contents/Resources"

cp "$binary_path" "$app_path/Contents/MacOS/$executable_name"
chmod 755 "$app_path/Contents/MacOS/$executable_name"

rm -rf "$iconset_path"
mkdir -p "$swift_cache_path"
CLANG_MODULE_CACHE_PATH="$swift_cache_path" \
  swift -module-cache-path "$swift_cache_path" \
  "$script_dir/render-app-icon.swift" "$iconset_path" "$app_path/Contents/Resources/$icon_name"

cat > "$app_path/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key>
  <string>en</string>
  <key>CFBundleExecutable</key>
  <string>${executable_name}</string>
  <key>CFBundleIdentifier</key>
  <string>${bundle_id}</string>
  <key>CFBundleIconFile</key>
  <string>${icon_name}</string>
  <key>CFBundleInfoDictionaryVersion</key>
  <string>6.0</string>
  <key>CFBundleName</key>
  <string>${bundle_name}</string>
  <key>CFBundleDisplayName</key>
  <string>${bundle_name}</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleShortVersionString</key>
  <string>${version}</string>
  <key>CFBundleVersion</key>
  <string>${version}</string>
  <key>LSApplicationCategoryType</key>
  <string>public.app-category.finance</string>
  <key>LSMinimumSystemVersion</key>
  <string>12.0</string>
  <key>NSHighResolutionCapable</key>
  <true/>
  <key>NSSupportsAutomaticGraphicsSwitching</key>
  <true/>
</dict>
</plist>
PLIST

plutil -lint "$app_path/Contents/Info.plist" >/dev/null
codesign --force --deep --sign - "$app_path" >/dev/null

echo "Created $app_path"
