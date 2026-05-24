#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${VERSION:-}" ]]; then
  echo "VERSION is required." >&2
  exit 1
fi

normalize_version() {
  local version="$1"
  version="${version#v}"
  version="${version#V}"
  version="${version%%+*}"
  echo "$version"
}

latest_version="$(
  gh release list --limit 1 --json tagName --jq '.[0].tagName // ""'
)"

should_release="true"
if [[ -n "$latest_version" ]]; then
  current_normalized="$(normalize_version "$VERSION")"
  latest_normalized="$(normalize_version "$latest_version")"

  oldest_version="$(printf '%s\n' "$current_normalized" "$latest_normalized" | sort -V | head -n1)"
  if [[ "$oldest_version" != "$latest_normalized" || "$current_normalized" == "$latest_normalized" ]]; then
    should_release="false"
  fi
fi

echo "latest_version=$latest_version" >>"$GITHUB_OUTPUT"
echo "should_release=$should_release" >>"$GITHUB_OUTPUT"

if [[ "$should_release" == "true" ]]; then
  echo "Release will be created for $VERSION. Latest release: $latest_version" >>"$GITHUB_STEP_SUMMARY"
else
  echo "Release skipped. Project version $VERSION is not greater than latest release $latest_version." >>"$GITHUB_STEP_SUMMARY"
fi

echo "version=$VERSION"
echo "latest_version=$latest_version"
echo "should_release=$should_release"
