#shec plugin

include_directories(.)

set(shec_utils_srcs
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc 
  ErasureCodePluginShec.cc 
  ErasureCodeShec.cc 
  ErasureCodeShecTableCache.cc 
  determinant.c)

add_library(shec_utils OBJECT ${shec_utils_srcs})
add_dependencies(shec_utils ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)

set(ec_shec_objs
  $<TARGET_OBJECTS:gf-complete_objs>
  $<TARGET_OBJECTS:jerasure_objs>
  $<TARGET_OBJECTS:shec_utils>)

add_library(ec_shec SHARED ${ec_shec_objs})
add_dependencies(ec_shec ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
set_target_properties(ec_shec PROPERTIES
  INSTALL_RPATH "")
target_link_libraries(ec_shec ${EXTRALIBS})
install(TARGETS ec_shec DESTINATION ${erasure_plugin_dir})

# legacy libraries
foreach(flavor ${jerasure_legacy_flavors})
  set(plugin_name "ec_shec_${flavor}")
  add_library(${plugin_name} SHARED ${ec_shec_objs})
  set_target_properties(${plugin_name} PROPERTIES
    INSTALL_RPATH "")
  install(TARGETS ${plugin_name} DESTINATION ${erasure_plugin_dir})
  add_dependencies(ec_shec ${plugin_name})
endforeach()

if(WITH_EMBEDDED)
  # note we rely on the fact this will always be statically linked with jerasure
  add_library(cephd_ec_shec STATIC ${shec_utils_srcs})
  set_target_properties(cephd_ec_shec PROPERTIES COMPILE_DEFINITIONS BUILDING_FOR_EMBEDDED)
endif()
