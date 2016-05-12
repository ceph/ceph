set(sphinx_input)
set(sphinx_output)
set(sphinx_output_dir ${CMAKE_BINARY_DIR}/doc/man)

add_subdirectory(8)

add_custom_command(
  OUTPUT ${sphinx_output}
  COMMAND ${SPHINX_BUILD} -b man -d ${CMAKE_BINARY_DIR}/doc/doctrees -c ${CMAKE_SOURCE_DIR}/man ${CMAKE_CURRENT_SOURCE_DIR} ${sphinx_output_dir}
  DEPENDS ${sphinx_input})

add_custom_target(
  manpages ALL
  DEPENDS ${sphinx_output}
  COMMENT "manpages building")
