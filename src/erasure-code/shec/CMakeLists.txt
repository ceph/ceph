#shec plugin

include_directories(.)

add_library(shec_utils OBJECT
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc 
  ErasureCodePluginShec.cc 
  ErasureCodeShec.cc 
  ErasureCodeShecTableCache.cc 
  determinant.c)
add_dependencies(shec_utils ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)

add_library(ec_shec SHARED ErasureCodePluginSelectShec.cc)
add_dependencies(ec_shec ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_shec ${EXTRALIBS})
install(TARGETS ec_shec DESTINATION ${erasure_plugin_dir})

foreach(flavor ${jerasure_flavors})
  set(plugin_name "ec_shec_${flavor}")
  add_library(${plugin_name} SHARED
    $<TARGET_OBJECTS:jerasure_${flavor}>
    $<TARGET_OBJECTS:shec_utils>)
  target_link_libraries(${plugin_name} ${EXTRALIBS})
  install(TARGETS ${plugin_name} DESTINATION ${erasure_plugin_dir})
endforeach(flavor flavors)
