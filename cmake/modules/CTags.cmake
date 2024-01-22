find_program(CTAGS_EXECUTABLE ctags)

function(add_tags name)
  cmake_parse_arguments(TAGS "" "SRC_DIR;TAG_FILE" "EXCLUDE_OPTS;EXCLUDES" ${ARGN})
  set(excludes ${TAGS_EXCLUDES})
  if(TAGS_EXCLUDE_OPTS)
    # always respect EXCLUDES_OPTS
    list(APPEND excludes ${TAGS_EXCLUDE_OPTS})
  else()
    # exclude the submodules under SRC_DIR by default
    execute_process(
      COMMAND git config --file .gitmodules --get-regexp path
      COMMAND awk "/${TAGS_SRC_DIR}/ { print $2 }"
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      RESULT_VARIABLE result_code
      OUTPUT_VARIABLE submodules
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(${result_code} EQUAL 0)
      string(REPLACE "${TAGS_SRC_DIR}/" "" submodules "${submodules}")
      # cmake list uses ";" as the delimiter, so split the string manually
      # before iterating in it.
      string(REPLACE "\n" ";" submodules "${submodules}")
      list(APPEND excludes ${submodules})
    endif()
  endif()
  message(STATUS "exclude following files under ${TAGS_SRC_DIR}: ${excludes}")
  # add_custom_target() accepts a list after "COMMAND" keyword, so we should
  # make exclude_arg a list, otherwise cmake will quote it. and ctags will
  # take it as as a single argument.
  foreach(exclude ${excludes})
    list(APPEND exclude_args --exclude=${exclude})
  endforeach()
  add_custom_target(${name}
    COMMAND ${CTAGS_EXECUTABLE} -R --python-kinds=-i --c++-kinds=+p --fields=+iaS --extra=+q ${exclude_args}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/${TAGS_SRC_DIR}
    COMMENT "Building ctags file ${TAGS_TAG_FILE}"
    VERBATIM)
  set_source_files_properties(${CMAKE_SOURCE_DIR}/${TAGS_TAG_FILE} PROPERTIES
    GENERATED true)
endfunction()
