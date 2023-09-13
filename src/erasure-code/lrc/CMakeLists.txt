# lrc plugin

set(lrc_srcs
  ErasureCodePluginLrc.cc
  ErasureCodeLrc.cc
  $<TARGET_OBJECTS:erasure_code_objs>
  $<TARGET_OBJECTS:crush_objs>
  ${CMAKE_SOURCE_DIR}/src/common/str_map.cc
)

add_library(ec_lrc SHARED ${lrc_srcs})
set_target_properties(ec_lrc PROPERTIES
  INSTALL_RPATH "")
target_link_libraries(ec_lrc json_spirit)
install(TARGETS ec_lrc DESTINATION ${erasure_plugin_dir})
