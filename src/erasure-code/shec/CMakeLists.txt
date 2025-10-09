#shec plugin

include_directories(.)

set(shec_utils_srcs
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc 
  ErasureCodePluginShec.cc 
  ErasureCodeShec.cc 
  ErasureCodeShecTableCache.cc 
  determinant.c)

add_library(shec_utils OBJECT ${shec_utils_srcs})
target_link_libraries(shec_utils
  PRIVATE legacy-option-headers)

set(ec_shec_objs
  $<TARGET_OBJECTS:gf-complete_objs>
  $<TARGET_OBJECTS:jerasure_objs>
  $<TARGET_OBJECTS:shec_utils>)

add_library(ec_shec SHARED ${ec_shec_objs})
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
