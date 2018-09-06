# clay plugin

set(clay_srcs
  ErasureCodePluginClay.cc
  ErasureCodeClay.cc
  $<TARGET_OBJECTS:erasure_code_objs>
  $<TARGET_OBJECTS:crush_objs>
  ${CMAKE_SOURCE_DIR}/src/common/str_map.cc
)

add_library(ec_clay SHARED ${clay_srcs})
#add_dependencies(ec_clay ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
set_target_properties(ec_clay PROPERTIES
  INSTALL_RPATH "")
target_link_libraries(ec_clay json_spirit)
install(TARGETS ec_clay DESTINATION ${erasure_plugin_dir})

if(WITH_EMBEDDED)
  add_library(cephd_ec_clay STATIC ${clay_srcs})
  set_target_properties(cephd_ec_clay PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
endif()
