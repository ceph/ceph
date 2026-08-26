#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/detect_host.sh"

OUTFILE="${1:-${SCRIPT_DIR}/host.json}"
bootstrap_local_presets "${SCRIPT_DIR}"
write_host_json "$OUTFILE"
echo "Wrote ${OUTFILE}"
