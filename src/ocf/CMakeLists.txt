# The root of the OCF resource agent hierarchy
# Per the OCF standard, it's always "lib",
# not "lib64" (even on 64-bit platforms).
set(ocf_dir ${CMAKE_INSTALL_PREFIX}/lib/ocf)

# The ceph provider directory
set(ra_dir ${ocf_dir}/resource.d/${PROJECT_NAME})

configure_file(rbd.in rbd @ONLY)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/rbd DESTINATION ${ra_dir})
