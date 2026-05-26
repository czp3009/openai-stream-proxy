#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${VERSION:-}" ]]; then
  echo "VERSION is required." >&2
  exit 1
fi

if ! compgen -G "release-assets/*" >/dev/null; then
  echo "No release assets found." >&2
  exit 1
fi

find release-assets -maxdepth 1 -type f -print | sort
gh release create "$VERSION" release-assets/* \
  --title "$VERSION" \
  --generate-notes \
  --target "$GITHUB_SHA"
