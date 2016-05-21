# The root of the OCF resource agent hierarchy
# Per the OCF standard, it's always "lib",
# not "lib64" (even on 64-bit platforms).
set(ocf_dir ${CMAKE_INSTALL_PREFIX}/lib/ocf)

string(TOLOWER ${PROJECT_NAME} lower_project_name)
# The ceph provider directory
set(ra_dir ${ocf_dir}/resource.d/${lower_project_name})

foreach(agent ceph rbd)
  configure_file(${agent}.in
    ${agent} @ONLY)
  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/${agent}
    DESTINATION ${ra_dir})
endforeach()

foreach(agent osd mds mon)
  install(CODE
    "EXECUTE_PROCESS(COMMAND ${CMAKE_COMMAND} -E create_symlink ceph ${agent}
       WORKING_DIRECTORY \"${ra_dir}\")")
endforeach()
