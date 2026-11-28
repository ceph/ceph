cmake_minimum_required(VERSION 3.22.1)

foreach(required IN ITEMS PYTHON_EXECUTABLE GENERATOR_SCRIPT WORK_DIR)
  if(NOT DEFINED ${required} OR "${${required}}" STREQUAL "")
    message(FATAL_ERROR "${required} is required")
  endif()
endforeach()

file(MAKE_DIRECTORY "${WORK_DIR}")
set(input "${WORK_DIR}/options.yaml")
set(source "${WORK_DIR}/options.cc")
set(header "${WORK_DIR}/options.h")
file(REMOVE "${source}" "${header}")

function(generate expect_failure)
  execute_process(
    COMMAND "${PYTHON_EXECUTABLE}" "${GENERATOR_SCRIPT}"
      --input "${input}" --output "${source}" --legacy "${header}" --name test ${ARGN}
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error)
  if(expect_failure)
    if(result STREQUAL "0")
      message(FATAL_ERROR "Invalid options were accepted")
    endif()
    return()
  endif()
  if(NOT result STREQUAL "0")
    message(FATAL_ERROR "Options generation failed: ${output}${error}")
  endif()
endfunction()

function(expect_unchanged path timestamp checksum)
  file(TIMESTAMP "${path}" actual_timestamp "%s")
  file(SHA256 "${path}" actual_checksum)
  if(NOT actual_timestamp STREQUAL timestamp OR NOT actual_checksum STREQUAL checksum)
    message(FATAL_ERROR "Unchanged output was rewritten: ${path}")
  endif()
endfunction()

function(expect_header expected)
  file(READ "${header}" actual)
  if(NOT actual STREQUAL expected)
    message(FATAL_ERROR "Unexpected legacy declarations: ${actual}")
  endif()
endfunction()

set(options "options:
- name: example
  type: int
  level: basic
  desc: Original description
  default: 1
  with_legacy: true
")
file(WRITE "${input}" "${options}")
generate(FALSE)
expect_header("OPTION(example, OPT_INT)\n")
file(TIMESTAMP "${source}" source_timestamp "%s")
file(TIMESTAMP "${header}" header_timestamp "%s")
file(SHA256 "${source}" source_checksum)
file(SHA256 "${header}" header_checksum)

# Separate writes across second-resolution timestamps before testing stability.
execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep 1)
generate(FALSE)
expect_unchanged("${source}" "${source_timestamp}" "${source_checksum}")
expect_unchanged("${header}" "${header_timestamp}" "${header_checksum}")

string(REPLACE "Original description" "Changed description" options "${options}")
file(WRITE "${input}" "${options}")
generate(FALSE)
expect_unchanged("${header}" "${header_timestamp}" "${header_checksum}")
file(SHA256 "${source}" description_checksum)
if(description_checksum STREQUAL source_checksum)
  message(FATAL_ERROR "Description change did not update the generated source")
endif()

string(REPLACE "default: 1" "default: 2" options "${options}")
file(WRITE "${input}" "${options}")
generate(FALSE)
expect_unchanged("${header}" "${header_timestamp}" "${header_checksum}")
file(SHA256 "${source}" default_checksum)
if(default_checksum STREQUAL description_checksum)
  message(FATAL_ERROR "Default change did not update the generated source")
endif()

string(REPLACE "type: int" "type: uint" options "${options}")
file(WRITE "${input}" "${options}")
generate(FALSE)
expect_header("OPTION(example, OPT_U32)\n")

string(REPLACE "name: example" "name: renamed" options "${options}")
file(WRITE "${input}" "${options}")
generate(FALSE)
expect_header("OPTION(renamed, OPT_U32)\n")

string(REPLACE "with_legacy: true" "with_legacy: false" without_legacy "${options}")
file(WRITE "${input}" "${without_legacy}")
generate(FALSE)
expect_header("")
file(WRITE "${input}" "${options}")
generate(FALSE)

file(TIMESTAMP "${source}" source_timestamp "%s")
file(SHA256 "${source}" source_checksum)
execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep 1)
file(REMOVE "${header}")
generate(FALSE)
expect_header("OPTION(renamed, OPT_U32)\n")
expect_unchanged("${source}" "${source_timestamp}" "${source_checksum}")

file(TIMESTAMP "${header}" header_timestamp "%s")
file(SHA256 "${header}" header_checksum)
execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep 1)
file(REMOVE "${source}")
generate(FALSE)
expect_unchanged("${header}" "${header_timestamp}" "${header_checksum}")
file(TIMESTAMP "${source}" source_timestamp "%s")
file(SHA256 "${source}" source_checksum)

# Conversion errors and malformed input must fail without publishing partial output.
string(REPLACE "default: 2" "default: invalid" invalid "${options}")
file(WRITE "${input}" "${invalid}")
generate(TRUE)
expect_unchanged("${source}" "${source_timestamp}" "${source_checksum}")
expect_unchanged("${header}" "${header_timestamp}" "${header_checksum}")

string(REPLACE "name: renamed" "name: renamed\n  name: duplicate" invalid "${options}")
file(WRITE "${input}" "${invalid}")
generate(TRUE)
expect_unchanged("${source}" "${source_timestamp}" "${source_checksum}")
expect_unchanged("${header}" "${header_timestamp}" "${header_checksum}")

# Raw fragments have the same unchanged-output and failed-generation guarantees.
file(WRITE "${input}" "${options}")
generate(FALSE --raw)
expect_header("OPTION(renamed, OPT_U32)\n")
file(READ "${source}" raw_source)
string(FIND "${raw_source}" "Option(\"renamed\"," declaration)
if(declaration LESS 0)
  message(FATAL_ERROR "Raw generation did not produce an option declaration")
endif()
file(TIMESTAMP "${source}" source_timestamp "%s")
file(SHA256 "${source}" source_checksum)
execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep 1)
generate(FALSE --raw)
expect_unchanged("${source}" "${source_timestamp}" "${source_checksum}")
expect_unchanged("${header}" "${header_timestamp}" "${header_checksum}")

file(WRITE "${input}" "${invalid}")
generate(TRUE --raw)
expect_unchanged("${source}" "${source_timestamp}" "${source_checksum}")
expect_unchanged("${header}" "${header_timestamp}" "${header_checksum}")
