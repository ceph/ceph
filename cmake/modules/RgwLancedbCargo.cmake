# Build locations shared by the two cargo invocations in the tree: the
# rgw-lancedb staticlib linked into radosgw (src/rgw/CMakeLists.txt) and the
# lancedb-rgw-store `cargo test` binary (src/test/rgw/CMakeLists.txt).
#
# They deliberately share one CARGO_TARGET_DIR so the test binary reuses the
# already-compiled lance/arrow dependencies instead of rebuilding them.  That
# also means the two cargo runs contend for the same lock, so the test target
# takes an explicit dependency on rgw-lancedb.
#
# This lives in a module rather than in src/rgw/CMakeLists.txt because src/test
# is processed first, so neither directory can rely on the other having run.
# No include_guard(): the file only sets variables, and each including
# directory needs its own copy of them.

set(RGW_LANCEDB_BUILD_DIR
  ${CMAKE_BINARY_DIR}/src/rgw/rgw-lancedb-prefix/src/rgw-lancedb-build)
set(RGW_LANCEDB_CARGO_TARGET_DIR ${RGW_LANCEDB_BUILD_DIR}/target)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(RGW_LANCEDB_CARGO_FLAG "")
  set(RGW_LANCEDB_TARGET_DIR ${RGW_LANCEDB_CARGO_TARGET_DIR}/debug)
else()
  set(RGW_LANCEDB_CARGO_FLAG "--release")
  set(RGW_LANCEDB_TARGET_DIR ${RGW_LANCEDB_CARGO_TARGET_DIR}/release)
endif()
