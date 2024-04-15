add_executable(ceph-erasure-code-tool
  ${PROJECT_SOURCE_DIR}/src/osd/ECUtil.cc
  ceph-erasure-code-tool.cc)
add_dependencies(ceph-erasure-code-tool prebuild-targets)
target_link_libraries(ceph-erasure-code-tool global ceph-common)
install(TARGETS ceph-erasure-code-tool DESTINATION bin)
