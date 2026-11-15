function(gperf_generate input output)
  get_filename_component(output "${output}" ABSOLUTE BASE_DIR "${CMAKE_CURRENT_BINARY_DIR}")
  set(update_timestamp)
  if(CMAKE_GENERATOR MATCHES "Makefiles")
    # Make does not restat unchanged outputs. Avoid regenerating on every build.
    set(update_timestamp COMMAND "${CMAKE_COMMAND}" -E touch_nocreate "${output}")
  endif()
  add_custom_command(
    OUTPUT "${output}"
    COMMAND "${CMAKE_COMMAND}"
      "-DGPERF=${GPERF}" "-DINPUT=${input}" "-DOUTPUT=${output}"
      -P "${CMAKE_CURRENT_FUNCTION_LIST_FILE}"
    ${update_timestamp}
    DEPENDS "${input}" "${GPERF}" "${CMAKE_CURRENT_FUNCTION_LIST_FILE}"
    COMMENT "Generate ${output}"
    VERBATIM)
endfunction()

if(NOT CMAKE_SCRIPT_MODE_FILE STREQUAL CMAKE_CURRENT_LIST_FILE)
  return()
endif()

foreach(required IN ITEMS GPERF INPUT OUTPUT)
  if(NOT DEFINED ${required} OR "${${required}}" STREQUAL "")
    message(FATAL_ERROR "${required} is required")
  endif()
endforeach()

execute_process(
  COMMAND "${GPERF}" "${INPUT}"
  RESULT_VARIABLE result
  OUTPUT_VARIABLE generated)
if(NOT result STREQUAL "0")
  message(FATAL_ERROR "gperf generation failed: ${result}")
endif()

string(REPLACE "register " "" generated "${generated}")
file(WRITE "${OUTPUT}.tmp" "${generated}")
configure_file("${OUTPUT}.tmp" "${OUTPUT}" COPYONLY)
file(REMOVE "${OUTPUT}.tmp")
