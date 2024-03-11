#!/bin/bash

set -e

function prun {
  printf '%s\n' "$*" >&2
  "$@"
}

partitions="$1"
shift

ARGS=()
ARGS+=("--subset=$((RANDOM % partitions))/$partitions")

if [ -n "$CEPH_QA_EMAIL" ]; then
  ARGS+=("--email=$CEPH_QA_EMAIL")
fi

prun teuthology-suite "${ARGS[@]}" $TEUTHOLOGY_SUITE_ARGS "$@"
