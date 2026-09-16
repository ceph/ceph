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
# Watch run_test_plan output in a loop log; emit PROGRESS lines on each phase/loop.
set -uo pipefail

LOG="${1:?log path}"
PID="${2:?runner pid}"
LOOPS="${3:-2}"
TOTAL="${4:-14}"

phase_name() {
  case "$1" in
    0) echo "build" ;;
    1) echo "unit tests" ;;
    2) echo "reload (clean)" ;;
    3) echo "smoke" ;;
    4) echo "byte range GET (4MiB)" ;;
    5) echo "list boundary (1024)" ;;
    6) echo "pagination (64K)" ;;
    7) echo "list stress (64K upload)" ;;
    8) echo "list buckets stress (1K buckets)" ;;
    9) echo "gc delete queue" ;;
    10) echo "kill instance" ;;
    10b) echo "reload restore (3)" ;;
    11) echo "power-cycle easy" ;;
    12) echo "power-cycle hard" ;;
    *) echo "phase $1" ;;
  esac
}

next_phase() {
  case "$1" in
    0) echo "unit tests" ;;
    1) echo "reload (clean)" ;;
    2) echo "smoke" ;;
    3) echo "byte range GET (4MiB)" ;;
    4) echo "list boundary (1024)" ;;
    5) echo "pagination (64K)" ;;
    6) echo "list stress (64K upload)" ;;
    7) echo "list buckets stress (1K buckets)" ;;
    8) echo "gc delete queue" ;;
    9) echo "kill instance" ;;
    10) echo "reload restore (3)" ;;
    10b) echo "power-cycle easy" ;;
    11) echo "power-cycle hard" ;;
    12) echo "(loop end)" ;;
    *) echo "unknown" ;;
  esac
}

SEEN=0
FAILS=0
LOOPS_DONE=0

emit() {
  echo "PROGRESS: $*"
}

count_matches() {
  grep -E "$1" "${LOG}" 2>/dev/null | wc -l | tr -d ' '
}

while kill -0 "${PID}" 2>/dev/null; do
  CUR=$(count_matches '^(PASS|FAIL):')
  if [[ "${CUR}" -gt "${SEEN}" ]]; then
    mapfile -t NEW_LINES < <(grep -E '^(PASS|FAIL):' "${LOG}" | tail -n "$((CUR - SEEN))")
    for line in "${NEW_LINES[@]}"; do
      kind="${line%%:*}"
      rest="${line#*: }"
      num="${rest%% *}"
      name="${rest#${num} }"
      [[ "${kind}" == FAIL ]] && FAILS=$((FAILS + 1))
      LOOPS_DONE=$(count_matches "^=== LOOP [0-9]+/${LOOPS} PASS")
      CUR_LOOP=$((LOOPS_DONE + 1))
      [[ "${CUR_LOOP}" -gt "${LOOPS}" ]] && CUR_LOOP="${LOOPS}"
      done_in_loop=$((SEEN + 1 - LOOPS_DONE * TOTAL))
      [[ "${done_in_loop}" -lt 0 ]] && done_in_loop=0
      [[ "${done_in_loop}" -gt "${TOTAL}" ]] && done_in_loop="${TOTAL}"
      next="$(next_phase "${num}")"
      if [[ "${next}" == "(loop end)" && "${CUR_LOOP}" -lt "${LOOPS}" ]]; then
        next="build (loop $((CUR_LOOP + 1)))"
      elif [[ "${next}" == "(loop end)" ]]; then
        next="(none — run complete)"
      fi
      emit "loop=${CUR_LOOP}/${LOOPS} finished=${name} outcome=${kind} next=${next} progress=${done_in_loop}/${TOTAL} failures=${FAILS}"
      SEEN=$((SEEN + 1))
    done
  fi
  NEW_LOOPS=$(count_matches "^=== LOOP [0-9]+/${LOOPS} PASS")
  if [[ "${NEW_LOOPS}" -gt "${LOOPS_DONE}" ]]; then
    LOOPS_DONE="${NEW_LOOPS}"
    emit "loop=${LOOPS_DONE}/${LOOPS} LOOP_PASS failures=${FAILS}"
  fi
  sleep 15
done

if grep -q "^=== ALL ${LOOPS} LOOPS PASS" "${LOG}" 2>/dev/null; then
  emit "ALL_PASS loops=${LOOPS}/${LOOPS} failures=${FAILS}"
elif grep -qE "^=== LOOP [0-9]+/${LOOPS} FAIL" "${LOG}" 2>/dev/null; then
  emit "STOPPED_ON_FAIL failures=${FAILS}"
else
  emit "RUNNER_EXITED failures=${FAILS}"
fi
