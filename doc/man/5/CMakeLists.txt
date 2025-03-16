if(WITH_RBD)
  list(APPEND man_srcs
	rbdmap.rst
	rbdtab.rst
  )
endif()

foreach(man ${man_srcs})
  list(APPEND sphinx_input ${CMAKE_CURRENT_SOURCE_DIR}/${man})
  # mount.ceph.rst => mount if we use
  # get_filename_component(cmd ${man} NAME_WE)
  string(REGEX REPLACE ".rst$" "" cmd ${man})
  list(APPEND sphinx_output ${sphinx_output_dir}/${cmd}.5)
  install(FILES ${sphinx_output_dir}/${cmd}.5
    DESTINATION ${CEPH_MAN_DIR}/man5)
endforeach()

set(sphinx_input ${sphinx_input} PARENT_SCOPE)
set(sphinx_output ${sphinx_output} PARENT_SCOPE)
