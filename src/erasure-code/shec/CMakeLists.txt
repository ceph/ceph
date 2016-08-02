#shec plugin

include_directories(.)

set(shec_objs_srcs
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc 
  ErasureCodePluginShec.cc 
  ErasureCodeShec.cc 
  ErasureCodeShecTableCache.cc 
  determinant.c 
  )

add_library(shec_objs OBJECT ${shec_objs_srcs})

add_library(ceph_shec_generic SHARED 
  $<TARGET_OBJECTS:jerasure_objs>
  $<TARGET_OBJECTS:shec_objs>)
target_link_libraries(ceph_shec_generic crush pthread)
add_dependencies(ceph_shec_generic ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
set_target_properties(ceph_shec_generic PROPERTIES VERSION 1.0.0 SOVERSION 1)
install(TARGETS ceph_shec_generic DESTINATION ${erasure_plugin_dir})

# ceph_shec_neon
#TODO:build libec_shec_neon library on an ARM machine
if(ARM_NEON OR ARM_NEON2)
  set(shec_neon_srcs
    $<TARGET_OBJECTS:shec_objs>
    $<TARGET_OBJECTS:jerasure_objs>
    $<TARGET_OBJECTS:neon_objs>
    )
  add_library(ceph_shec_neon SHARED ${shec_neon_srcs})
  add_dependencies(ceph_shec_neon ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ceph_shec_neon ${EXTRALIBS})
  set_target_properties(ceph_shec_neon PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${ARM_NEON_FLAGS})
  install(TARGETS ceph_shec_neon DESTINATION ${erasure_plugin_dir})
endif(ARM_NEON OR ARM_NEON2)

# ceph_shec_sse3
if(INTEL_SSE)
  add_library(ceph_shec_sse3 SHARED 
    $<TARGET_OBJECTS:jerasure_objs>
    $<TARGET_OBJECTS:shec_objs>)
  add_dependencies(ceph_shec_sse3 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ceph_shec_sse3 crush ${EXTRALIBS})
  set_target_properties(ceph_shec_sse3 PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${SSE3_FLAGS})
  install(TARGETS ceph_shec_sse3 DESTINATION ${erasure_plugin_dir})
endif(INTEL_SSE)

# ceph_shec_sse4
if(INTEL_SSE4_1)
  add_library(ceph_shec_sse4 SHARED 
    $<TARGET_OBJECTS:jerasure_objs>
    $<TARGET_OBJECTS:shec_objs>)
  add_dependencies(ceph_shec_sse4 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ceph_shec_sse4 crush ${EXTRALIBS})
  set_target_properties(ceph_shec_sse4 PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${SSE4_FLAGS})
  install(TARGETS ceph_shec_sse4 DESTINATION ${erasure_plugin_dir})
endif(INTEL_SSE4_1)

add_library(ceph_shec SHARED ErasureCodePluginSelectShec.cc)
add_dependencies(ceph_shec ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_shec ${EXTRALIBS})
set_target_properties(ceph_shec PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ceph_shec DESTINATION ${erasure_plugin_dir})
