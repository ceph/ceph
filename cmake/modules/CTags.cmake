find_program(CTAGS_EXECUTABLE ctags)
if(CTAGS_EXECUTABLE)
  set(src_dir "src")
  set(tag_file ${src_dir}/tags)
  execute_process(
    COMMAND git config --file .gitmodules --get-regexp path
    COMMAND awk "/${src_dir}/ { print $2 }"
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE submodules
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  string(REPLACE "\n" " " submodules ${submodules})
  string(REPLACE "${src_dir}/" "--exclude=" excludes ${submodules})
  # cmake will quote any embedded spaces when expanding ${excludes}
  # in the command below, so, turn ${excludes} into a cmake List (separated
  # by semicolons) to let the expansion below simply replace them with spaces
  # again.
  separate_arguments(excludes UNIX_COMMAND ${excludes})
  add_custom_target(ctags
    COMMAND ${CTAGS_EXECUTABLE} -R --c++-kinds=+p --fields=+iaS --extra=+q --exclude=*.js ${excludes}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/${src_dir}
    COMMENT "Building ctags file ${tag_file} (no submodules)"
    VERBATIM)
  add_custom_target(tags
    DEPENDS ctags)
  set_source_files_properties(${CMAKE_SOURCE_DIR}/${tag_file} PROPERTIES
    GENERATED true)
endif(CTAGS_EXECUTABLE)
