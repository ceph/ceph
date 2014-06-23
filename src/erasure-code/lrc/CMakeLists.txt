# LRC plugin

set(lrc_srcs
  ErasureCodePluginLrc.cc
  ErasureCodeLrc.cc
  $<TARGET_OBJECTS:erasure_code_objs>
)

add_library(ec_LRC SHARED ${lrc_srcs})
add_dependencies(ec_LRC ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
set_target_properties(ec_LRC PROPERTIES VERSION 1.0.0 SOVERSION 1)
install(TARGETS ec_LRC DESTINATION lib/erasure-code)
