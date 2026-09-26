#!/bin/bash
#
# Ceph - scalable distributed file system
#
# Author: Gabriel BenHanokh <gbenhano@redhat.com>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
#!/usr/bin/env bash
# Create N files filled with random data under DIR.
#
# Usage:
#   mk_random_files.sh -n COUNT [-s SIZE] [-p PREFIX] DIR
#
# Examples:
#   mk_random_files.sh -n 100 /tmp/blobs
#   mk_random_files.sh -n 50 -s 64K -p obj ./data/test

set -euo pipefail

SIZE=4096
PREFIX="file"
COUNT=""

usage() {
  cat <<EOF
Usage: $(basename "$0") -n COUNT [-s SIZE] [-p PREFIX] DIR

  DIR     Output directory (created if missing)

Options:
  -n NUM   Number of files to create (required). Suffixes: K, M, G (e.g. 10K)
  -s SIZE  Bytes per file (default: 4096). Suffixes: K, M, G (e.g. 64K, 1M)
  -p NAME  Filename prefix (default: file) -> \${PREFIX}-\${N}
  -h       Show this help
EOF
}

parse_size() {
  local raw="$1"
  case "${raw}" in
    *[kK]) echo $(( ${raw%[kK]} * 1024 )) ;;
    *[mM]) echo $(( ${raw%[mM]} * 1024 * 1024 )) ;;
    *[gG]) echo $(( ${raw%[gG]} * 1024 * 1024 * 1024 )) ;;
    *)     echo "${raw}" ;;
  esac
}

while getopts ":n:s:p:h" opt; do
  case "${opt}" in
    n) COUNT="$(parse_size "${OPTARG}")" ;;
    s) SIZE="$(parse_size "${OPTARG}")" ;;
    p) PREFIX="${OPTARG}" ;;
    h) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done
shift $((OPTIND - 1))

if [[ -z "${COUNT}" ]]; then
  echo "Error: -n COUNT is required" >&2
  usage >&2
  exit 2
fi

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 2
fi

DIR="$1"

if ! [[ "${COUNT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "COUNT must be a positive integer, got: ${COUNT}" >&2
  exit 2
fi

if ! [[ "${SIZE}" =~ ^[1-9][0-9]*$ ]]; then
  echo "Invalid size: ${SIZE}" >&2
  exit 2
fi

mkdir -p "${DIR}"

width="${#COUNT}"
for ((i = 1; i <= COUNT; i++)); do
  name=$(printf "%s-%0*d" "${PREFIX}" "${width}" "${i}")
  path="${DIR%/}/${name}"
  dd if=/dev/urandom of="${path}" bs="${SIZE}" count=1 status=none
done

echo "Created ${COUNT} file(s) (${SIZE} bytes each) in ${DIR}"
