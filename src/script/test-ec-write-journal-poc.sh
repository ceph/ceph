#!/usr/bin/env bash
# Focused developer build using an existing, compatible Ceph dependency build.
# No existing sources/build artifacts are modified. Prefer the normal CMake
# unittest_ec_write_journal target when a full build is configured.
set -euo pipefail
ulimit -c 0 # intentional assertion/death tests must not create large core files

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
: "${CEPH_DEPS_SOURCE:?Set to the Ceph checkout containing fmt and googletest}"
: "${CEPH_DEPS_BUILD:?Set to its compatible configured/built Ceph directory}"
build=${EC_JOURNAL_TEST_BUILD:-"$root/build-journal-poc"}
cxx=${CXX:-g++}

for input in \
  "$CEPH_DEPS_BUILD/include/acconfig.h" \
  "$CEPH_DEPS_BUILD/src/include/ceph_release.h" \
  "$CEPH_DEPS_BUILD/lib/libceph-common.so" \
  "$CEPH_DEPS_SOURCE/src/fmt/include/fmt/format.h" \
  "$CEPH_DEPS_SOURCE/src/googletest/googletest/src/gtest-all.cc"; do
  if [[ ! -f "$input" ]]; then
    printf 'Missing build dependency: %s\n' "$input" >&2
    exit 1
  fi
done

mkdir -p "$build"
flags=(-std=c++20 -g -O1 -Wall -Wextra -Wno-unused-parameter
  -Wno-unknown-pragmas -Wno-ignored-qualifiers -Wno-deprecated-copy -pthread
       -D_GNU_SOURCE -DHAVE_CONFIG_H)
extra_sources=()
if [[ ${EC_JOURNAL_FLUSH_TESTS:-0} == 1 ]]; then
  # Only retain the planner/extent-map functions needed by these tests. This
  # avoids requiring an entire previously built libosd for the focused build.
  flags+=(-ffunction-sections -fdata-sections -Wl,--gc-sections
    -DCEPH_DEBUG_MUTEX -DBOOST_ALLOW_DEPRECATED_HEADERS
    -I"$CEPH_DEPS_SOURCE/src/jaegertracing/opentelemetry-cpp/api/include"
    -I"$CEPH_DEPS_SOURCE/src/jaegertracing/opentelemetry-cpp/exporters/jaeger/include"
    -I"$CEPH_DEPS_SOURCE/src/jaegertracing/opentelemetry-cpp/ext/include"
    -I"$CEPH_DEPS_SOURCE/src/jaegertracing/opentelemetry-cpp/sdk/include")
  # os/Transaction.cc: the generator dumps per-shard transactions at debug
  # level 20, which the shard-set test links even though it never logs.
  extra_sources+=("$root/src/osd/ECJournalFlush.cc"
     "$root/src/osd/ECTransaction.cc"
     "$root/src/osd/ECUtil.cc"
     "$root/src/os/Transaction.cc"
     "$root/src/test/osd/test_ec_journal_flush.cc")
fi
if [[ ${EC_JOURNAL_SANITIZE:-0} == 1 ]]; then
  flags+=(-fsanitize=address,undefined -fno-omit-frame-pointer)
fi
if [[ -n ${BOOST_ROOT:-} ]]; then
  flags+=(-I"$BOOST_ROOT/include")
fi

# Compile the actual classic backend without requiring all libosd link inputs.
# Use only with headers/dependencies from this same Ceph base revision.
if [[ ${EC_JOURNAL_BACKEND_CHECK:-0} == 1 ]]; then
  # The reused RGW-only build may not have generated OSD tracepoint headers.
  # Generate them in our own ignored output directory, never in the old build.
  mkdir -p "$build/include/tracing"
  for trace in osd pg; do
    lttng-gen-tp "$root/src/tracing/$trace.tp" \
      -o "$build/include/tracing/$trace.h"
  done
  "$cxx" "${flags[@]}" -fsyntax-only -Werror -Wno-deprecated-declarations \
    -Wno-missing-field-initializers \
    -DCEPH_DEBUG_MUTEX -DBOOST_ALLOW_DEPRECATED_HEADERS \
    -DBOOST_MPL_CFG_NO_PREPROCESSED_HEADERS -DBOOST_MPL_LIMIT_LIST_SIZE=30 \
    -DBOOST_ASIO_NO_TS_EXECUTORS \
    -I"$build/include" \
    -I"$root/src" -I"$root/src/include" \
    -I"$CEPH_DEPS_BUILD/src/include" -I"$CEPH_DEPS_BUILD/include" \
    -I"$CEPH_DEPS_BUILD/src" -I"$CEPH_DEPS_SOURCE/src" \
    -I"$CEPH_DEPS_SOURCE/src/fmt/include" \
    -I"$CEPH_DEPS_SOURCE/src/xxHash" \
    -I"$CEPH_DEPS_SOURCE/src/dmclock/src" \
    -I"$CEPH_DEPS_SOURCE/src/dmclock/support/src" \
    -I"$CEPH_DEPS_SOURCE/src/rocksdb/include" \
    -I"$CEPH_DEPS_SOURCE/src/jaegertracing/opentelemetry-cpp/api/include" \
    -I"$CEPH_DEPS_SOURCE/src/jaegertracing/opentelemetry-cpp/exporters/jaeger/include" \
    -I"$CEPH_DEPS_SOURCE/src/jaegertracing/opentelemetry-cpp/ext/include" \
    -I"$CEPH_DEPS_SOURCE/src/jaegertracing/opentelemetry-cpp/sdk/include" \
    "$root/src/osd/ECBackend.cc" "$root/src/osd/ECJournalFlush.cc" \
    "$root/src/osd/ECCommon.cc" "$root/src/osd/ECTransaction.cc" \
    "$root/src/osd/ECWriteJournal.cc" "$root/src/osd/PrimaryLogPG.cc" \
    "$root/src/osd/OSD.cc"
fi

# Build gtest from source rather than reusing archives built by another GCC:
# GCC LTO bytecode is not compatible across major compiler versions.
"$cxx" "${flags[@]}" \
  -I"$root/src" -I"$root/src/include" \
  -I"$CEPH_DEPS_BUILD/src/include" -I"$CEPH_DEPS_BUILD/include" \
  -I"$CEPH_DEPS_SOURCE/src/fmt/include" \
  -I"$CEPH_DEPS_SOURCE/src/googletest/googletest/include" \
  -I"$CEPH_DEPS_SOURCE/src/googletest/googletest" \
  "$root/src/osd/ECWriteJournal.cc" \
  "$root/src/test/osd/test_ec_write_journal.cc" \
  "${extra_sources[@]}" \
  "$CEPH_DEPS_SOURCE/src/googletest/googletest/src/gtest-all.cc" \
  "$CEPH_DEPS_SOURCE/src/googletest/googletest/src/gtest_main.cc" \
  -L"$CEPH_DEPS_BUILD/lib" -Wl,-rpath,"$CEPH_DEPS_BUILD/lib" \
  -lceph-common -o "$build/unittest_ec_write_journal"

"$build/unittest_ec_write_journal" "$@"