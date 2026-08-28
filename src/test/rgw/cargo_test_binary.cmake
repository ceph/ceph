# Build a Rust integration-test binary with `cargo test --no-run` and copy it
# to a stable path, so it can be installed like any other ceph_test_* binary.
#
# Doing this at build time rather than at test time is what lets teuthology
# nodes run the tests without a source tree, a rust toolchain, or crates.io.
#
# Invoked with `cmake -P`.  Required variables:
#   CARGO         cargo executable
#   MANIFEST      path to the crate's Cargo.toml
#   TEST_TARGET   integration test target name (tests/<name>.rs), or "lib" to
#                 build the crate's own #[cfg(test)] unit tests
#   TARGET_DIR    CARGO_TARGET_DIR, shared with the rgw-lancedb build
#   LINK_DIR      directory holding libceph_rgw_sal_test_env.so
#   OUTPUT        where to copy the resulting binary
# Optional:
#   PROFILE_FLAG  e.g. --release, to match the ceph build type
#   RPATH         extra runtime search paths, ';'-separated

foreach(_required CARGO MANIFEST TEST_TARGET TARGET_DIR LINK_DIR OUTPUT)
  if(NOT ${_required})
    message(FATAL_ERROR "cargo_test_binary.cmake: ${_required} is not set")
  endif()
endforeach()

set(_cargo_flags)
if(PROFILE_FLAG)
  list(APPEND _cargo_flags ${PROFILE_FLAG})
endif()

if(TEST_TARGET STREQUAL "lib")
  list(APPEND _cargo_flags --lib)
else()
  list(APPEND _cargo_flags --test ${TEST_TARGET})
endif()

set(_env
  CARGO_TARGET_DIR=${TARGET_DIR}
  RGW_SAL_TEST_ENV_DIR=${LINK_DIR})
if(RPATH)
  string(REPLACE ";" ":" _rpath "${RPATH}")
  list(APPEND _env RGW_SAL_TEST_ENV_RPATH=${_rpath})
endif()

# json-render-diagnostics keeps stdout pure JSON while cargo renders rustc
# diagnostics to stderr, so a compile error is still readable in the build log.
set(_json "${OUTPUT}.cargo-artifacts.json")
execute_process(
  COMMAND ${CMAKE_COMMAND} -E env ${_env}
          ${CARGO} test --no-run ${_cargo_flags}
          --manifest-path ${MANIFEST}
          --message-format=json-render-diagnostics
  OUTPUT_FILE ${_json}
  RESULT_VARIABLE _rc
  COMMAND_ECHO STDERR)
if(NOT _rc EQUAL 0)
  message(FATAL_ERROR "cargo test --no-run failed with status ${_rc}")
endif()

# Only the test target itself yields a non-null "executable"; the library and
# build-script artifacts report null.
file(READ ${_json} _artifacts)
string(REGEX MATCHALL "\"executable\":\"[^\"]+\"" _matches "${_artifacts}")
if(NOT _matches)
  message(FATAL_ERROR
    "cargo produced no test executable for target '${TEST_TARGET}'; "
    "see ${_json}")
endif()
list(GET _matches -1 _match)
string(REGEX REPLACE "^\"executable\":\"(.*)\"$" "\\1" _executable "${_match}")

file(COPY_FILE "${_executable}" "${OUTPUT}" ONLY_IF_DIFFERENT)
