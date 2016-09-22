#shec plugin

include_directories(.)

add_library(shec_utils OBJECT
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc 
  ErasureCodePluginShec.cc 
  ErasureCodeShec.cc 
  ErasureCodeShecTableCache.cc 
  determinant.c)
add_dependencies(shec_utils ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)

add_library(ec_shec SHARED
  $<TARGET_OBJECTS:gf-complete_objs>
  $<TARGET_OBJECTS:jerasure_objs>
  $<TARGET_OBJECTS:shec_utils>)
add_dependencies(ec_shec ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_shec ${EXTRALIBS})
install(TARGETS ec_shec DESTINATION ${erasure_plugin_dir})
