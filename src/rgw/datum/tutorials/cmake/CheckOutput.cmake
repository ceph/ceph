execute_process(
  COMMAND "${EXECUTABLE}"
  RESULT_VARIABLE status
  OUTPUT_VARIABLE actual
  ERROR_VARIABLE diagnostics
  TIMEOUT 10)

if(NOT "0" STREQUAL "${status}")
  message(FATAL_ERROR "${EXECUTABLE} failed: ${status}\n${diagnostics}")
endif()
if(NOT "" STREQUAL "${diagnostics}")
  message(FATAL_ERROR "${EXECUTABLE} wrote to stderr:\n${diagnostics}")
endif()

file(READ "${EXPECTED_OUTPUT}" expected)
if(NOT "${expected}" STREQUAL "${actual}")
  message(FATAL_ERROR "${EXECUTABLE} output differs:\nExpected: [${expected}]\nActual: [${actual}]")
endif()
