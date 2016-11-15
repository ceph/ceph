#shec plugin

include_directories(.)

add_library(shec_utils OBJECT
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc 
  ErasureCodePluginShec.cc 
  ErasureCodeShec.cc 
  ErasureCodeShecTableCache.cc 
  determinant.c)
add_dependencies(shec_utils ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)

set(ec_shec_objs
  $<TARGET_OBJECTS:gf-complete_objs>
  $<TARGET_OBJECTS:jerasure_objs>
  $<TARGET_OBJECTS:shec_utils>)

add_library(ec_shec SHARED ${ec_shec_objs})
add_dependencies(ec_shec ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_shec ${EXTRALIBS})
install(TARGETS ec_shec DESTINATION ${erasure_plugin_dir})

# legacy libraries
foreach(flavor ${jerasure_legacy_flavors})
  set(plugin_name "ec_shec_${flavor}")
  add_library(${plugin_name} SHARED ${ec_shec_objs})
  install(TARGETS ${plugin_name} DESTINATION ${erasure_plugin_dir})
  add_dependencies(ec_shec ${plugin_name})
endforeach()
