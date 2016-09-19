# lrc plugin

set(lrc_srcs
  ErasureCodePluginLrc.cc
  ErasureCodeLrc.cc
  $<TARGET_OBJECTS:erasure_code_objs>
  ${CMAKE_SOURCE_DIR}/src/common/str_map.cc
)

add_library(ec_lrc SHARED ${lrc_srcs})
add_dependencies(ec_lrc ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_lrc crush json_spirit)
install(TARGETS ec_lrc DESTINATION ${erasure_plugin_dir})
