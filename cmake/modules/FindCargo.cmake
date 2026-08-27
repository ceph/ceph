# - Find Cargo
# Sets the following:
#
# CARGO_EXECUTABLE
# CARGO_VERSION
# Cargo_FOUND

find_program(CARGO_EXECUTABLE cargo)

if(CARGO_EXECUTABLE)
  execute_process(
    COMMAND ${CARGO_EXECUTABLE} --version
    OUTPUT_VARIABLE _cargo_version_output
    ERROR_QUIET
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  # ex: "cargo 1.91.0 (840b83a10 2025-07-30)"
  string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+" CARGO_VERSION "${_cargo_version_output}")
endif()

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(Cargo
  REQUIRED_VARS CARGO_EXECUTABLE
  VERSION_VAR CARGO_VERSION)

mark_as_advanced(
  CARGO_EXECUTABLE
  CARGO_VERSION)
