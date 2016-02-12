# lrc plugin

set(lrc_srcs
  ErasureCodePluginLrc.cc
  ErasureCodeLrc.cc
  $<TARGET_OBJECTS:erasure_code_objs>
  ${CMAKE_SOURCE_DIR}/src/common/str_map.cc
)

add_library(ceph_lrc SHARED ${lrc_srcs})
add_dependencies(ceph_lrc ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_lrc crush json_spirit)
set_target_properties(ceph_lrc PROPERTIES VERSION 1.0.0 SOVERSION 1)
install(TARGETS ceph_lrc DESTINATION ${erasure_plugin_dir})
