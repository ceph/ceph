# Build a Rust `cargo test` binary and expose it as a ceph test executable.
#
# libtest owns main(), so tests that need a ceph context / SAL driver get it from
# libceph_rgw_sal_test_env.so, which the test binary links against (see
# lancedb-rgw-store/build.rs, keyed on RGW_SAL_TEST_ENV_DIR).
#
# This one file plays two roles:
#
#   * include()d from CMakeLists.txt  -> defines add_rgw_cargo_test_binary().
#   * run via `cmake -P` (build time) -> does the actual `cargo test --no-run`
#     and copies the hash-suffixed output to a stable path.  The build-time work
#     has to happen in a script (add_custom_command runs commands, not cmake
#     code), and CMAKE_SCRIPT_MODE_FILE is set only in that `-P` invocation, so
#     we branch on it below.  No Python helper required.
#
# Two flavours, selected by TEST vs UNIT_TEST:
#
#   TEST <name>  a tests/<name>.rs integration test.  Built as part of ALL and
#                installed under bin/ so it runs on teuthology nodes with neither
#                a source tree nor a rust toolchain.
#   UNIT_TEST    the crate's own #[cfg(test)] unit tests (cargo's --lib binary).
#                Registered with add_ceph_test so it runs under `make check`; not
#                built by ALL and not installed, since it needs no cluster.
#
# add_rgw_cargo_test_binary(
#   NAME      <output binary name>
#   PACKAGE   <cargo package that owns the test>
#   TEST      <integration test name (tests/<TEST>.rs)>  # omit with UNIT_TEST
#   UNIT_TEST                                             # build --lib instead
#   DEPENDS   <cmake targets that must build first, e.g. the test-env .so>)

if(CMAKE_SCRIPT_MODE_FILE)
  # ---- build-time runner (invoked as `cmake -D... -P cargo_test_binary.cmake`) ----
  #
  # libtest names its output target/<profile>/deps/<name>-<hash>, where <hash>
  # changes across builds, so cmake cannot depend on a fixed path.  We ask cargo
  # for the artifact path via the JSON message stream and copy the reported
  # executable to OUTPUT.  The link-steering env vars (RGW_SAL_TEST_ENV_DIR,
  # RGW_SAL_TEST_ENV_RPATH, CARGO_TARGET_DIR) are read by build.rs from the
  # inherited environment; the caller sets them via `cmake -E env`.
  #
  # Required -D variables: CARGO MANIFEST PACKAGE MODE OUTPUT
  #   MODE = "lib" (crate #[cfg(test)] unit tests) or "test" (integration test)
  # Required when MODE=test: TEST_NAME (tests/<TEST_NAME>.rs)
  # Optional: PROFILE_FLAG (e.g. --release, to match the ceph build type)

  foreach(_required CARGO MANIFEST PACKAGE MODE OUTPUT)
    if(NOT ${_required})
      message(FATAL_ERROR "cargo_test_binary.cmake: ${_required} is not set")
    endif()
  endforeach()

  set(_flags)
  if(PROFILE_FLAG)
    list(APPEND _flags ${PROFILE_FLAG})
  endif()
  if(MODE STREQUAL "lib")
    list(APPEND _flags --lib)
  elseif(MODE STREQUAL "test")
    if(NOT TEST_NAME)
      message(FATAL_ERROR "cargo_test_binary.cmake: TEST_NAME is required when MODE=test")
    endif()
    list(APPEND _flags --test ${TEST_NAME})
  else()
    message(FATAL_ERROR "cargo_test_binary.cmake: MODE must be 'lib' or 'test', got '${MODE}'")
  endif()

  # json-render-diagnostics keeps stdout pure JSON (the artifact stream we parse)
  # while cargo renders rustc diagnostics to stderr, so a compile error stays
  # readable in the build log.
  set(_json "${OUTPUT}.cargo-artifacts.json")
  execute_process(
    COMMAND ${CARGO} test --no-run
            --message-format=json-render-diagnostics
            --manifest-path ${MANIFEST}
            -p ${PACKAGE}
            ${_flags}
    OUTPUT_FILE ${_json}
    RESULT_VARIABLE _rc
    COMMAND_ECHO STDERR)
  if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "cargo test --no-run failed with status ${_rc}")
  endif()

  # Only a target compiled as a test harness reports a non-null "executable"; the
  # plain library and build-script artifacts report null.  --lib / --test each
  # restrict the build to a single such target, so the last match is the binary
  # we want (works regardless of crate-type, which here is staticlib/rlib).
  file(READ ${_json} _artifacts)
  string(REGEX MATCHALL "\"executable\":\"[^\"]+\"" _matches "${_artifacts}")
  if(NOT _matches)
    message(FATAL_ERROR
      "cargo produced no test executable for '${MODE} ${TEST_NAME}'; see ${_json}")
  endif()
  list(GET _matches -1 _match)
  string(REGEX REPLACE "^\"executable\":\"(.*)\"$" "\\1" _executable "${_match}")

  file(COPY_FILE "${_executable}" "${OUTPUT}" ONLY_IF_DIFFERENT)
  return()
endif()

# ---- configure-time: define the function ----

function(add_rgw_cargo_test_binary)
  cmake_parse_arguments(CT "UNIT_TEST" "NAME;PACKAGE;TEST" "DEPENDS" ${ARGN})

  if(CT_UNIT_TEST AND CT_TEST)
    message(FATAL_ERROR "add_rgw_cargo_test_binary: TEST and UNIT_TEST are mutually exclusive")
  elseif(NOT CT_UNIT_TEST AND NOT CT_TEST)
    message(FATAL_ERROR "add_rgw_cargo_test_binary: one of TEST or UNIT_TEST is required")
  endif()

  set(_output "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${CT_NAME}")
  set(_manifest "${CMAKE_SOURCE_DIR}/src/rgw/${CT_PACKAGE}/Cargo.toml")

  # --lib for the crate unit tests, --test <name> for an integration target.
  if(CT_UNIT_TEST)
    set(_mode lib)
  else()
    set(_mode test)
  endif()

  # A target dir dedicated to the test build.  It must NOT be shared with the
  # umbrella rgw-lancedb build: build.rs gates its extra link args on
  # RGW_SAL_TEST_ENV_DIR (set here, unset there), so a shared dir would rerun
  # build.rs and recompile on every switch between the two.  The integration and
  # unit-test builds do share it -- they set identical env, so there is no such
  # churn, and cargo's own target-dir lock serializes the two cargo runs.
  set(_target_dir "${CMAKE_CURRENT_BINARY_DIR}/cargo-test-target")

  # Same profile rule as src/rgw/CMakeLists.txt: release unless a Debug build.
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(_release_flag "")
  else()
    set(_release_flag "--release")
  endif()

  # Rebuild when any crate or test source changes; cargo is incremental so a
  # no-op run is cheap, but this keeps the dependency graph honest for cmake.
  file(GLOB _srcs CONFIGURE_DEPENDS
    "${CMAKE_SOURCE_DIR}/src/rgw/${CT_PACKAGE}/src/*.rs"
    "${CMAKE_SOURCE_DIR}/src/rgw/${CT_PACKAGE}/tests/*.rs")

  # This same file, re-invoked in script mode to do the build-time work.
  set(_runner "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/cargo_test_binary.cmake")

  add_custom_command(
    OUTPUT ${_output}
    COMMAND ${CMAKE_COMMAND} -E env
      CARGO_TARGET_DIR=${_target_dir}
      RGW_SAL_TEST_ENV_DIR=${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
      RGW_SAL_TEST_ENV_RPATH=${CMAKE_INSTALL_FULL_LIBDIR}
      ${CMAKE_COMMAND}
        -DCARGO=${CARGO_EXECUTABLE}
        -DMANIFEST=${_manifest}
        -DPACKAGE=${CT_PACKAGE}
        -DMODE=${_mode}
        -DTEST_NAME=${CT_TEST}
        -DPROFILE_FLAG=${_release_flag}
        -DOUTPUT=${_output}
        -P ${_runner}
    DEPENDS ${_srcs} ${_runner}
    COMMENT "Building cargo test binary ${CT_NAME}"
    VERBATIM)

  if(CT_UNIT_TEST)
    # Not part of ALL; add_ceph_test hangs it off the `tests` target and runs it
    # under `make check`/ctest.  The build tree's lib dir is on LD_LIBRARY_PATH
    # there (and baked into the binary's rpath by build.rs), so the .so resolves
    # without an install.
    add_custom_target(${CT_NAME} DEPENDS ${_output})
    add_ceph_test(${CT_NAME} ${_output})
  else()
    add_custom_target(${CT_NAME} ALL DEPENDS ${_output})
    install(PROGRAMS ${_output} DESTINATION ${CMAKE_INSTALL_BINDIR})
  endif()

  if(CT_DEPENDS)
    add_dependencies(${CT_NAME} ${CT_DEPENDS})
  endif()
endfunction()
