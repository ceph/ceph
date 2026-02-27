#!/usr/bin/env bash
set -euo pipefail

WORKFLOWS_DIR="${1:-.}/.github/workflows"

echo "Scanning workflows in: $WORKFLOWS_DIR"

# Recursively grep workflow files for actions not pinned to SHA-1
grep -Prno --include="*.yml" --include="*.yaml" 'uses:\s*([^/]+)/([^@]+)@([^[:space:]]+)' "${WORKFLOWS_DIR}" | \
  while IFS=: read -r file _line_num uses_line; do
    echo -n "$file - "
    # Extract owner/repo/version
    if [[ "$uses_line" =~ uses:\ ([^/]+)/([^@]+)@([^[:space:]]+) ]]; then
        owner="${BASH_REMATCH[1]}"
        repo="${BASH_REMATCH[2]}"
        version="${BASH_REMATCH[3]}"
        action="$owner/$repo"
        echo -n "$owner/$repo: "
    else
        echo "Failed to parse line: $uses_line [FAIL]"
        continue
    fi

    # Skip if already pinned to SHA
    if [[ "$version" =~ ^[0-9a-f]{40}$ ]]; then
        echo "SHA-1 pinned: $version [OK]"
        continue
    else
        echo -n "Tag pinned: $version [WARNING], "
    fi

    api_url="https://api.github.com/repos/$owner/$repo/git/ref/tags/$version"

    # Get full SHA
    sha=$(curl -s "$api_url" | jq -r '.object.sha')
    if [[ "$sha" == "null" || -z "$sha" ]]; then
        echo "Could not resolve $action@$version [FAIL]"
        continue
    fi

    echo "Replacing $version → $sha [OK]"

    # Precise sed replacement: match 'uses:' literally and append comment
    sed -i.bak "s|uses:\s*$action@$version|uses: $action@$sha # $version|g" "$file"
done
