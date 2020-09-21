# clay plugin

set(clay_srcs
  ErasureCodePluginClay.cc
  ErasureCodeClay.cc
  $<TARGET_OBJECTS:erasure_code_objs>
  $<TARGET_OBJECTS:crush_objs>
  ${CMAKE_SOURCE_DIR}/src/common/str_map.cc
)

add_library(ec_clay SHARED ${clay_srcs})
set_target_properties(ec_clay PROPERTIES
  INSTALL_RPATH "")
install(TARGETS ec_clay DESTINATION ${erasure_plugin_dir})
