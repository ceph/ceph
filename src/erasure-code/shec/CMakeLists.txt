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

add_library(ec_shec_generic SHARED 
  $<TARGET_OBJECTS:jerasure_objs>
  $<TARGET_OBJECTS:shec_objs>)
target_link_libraries(ec_shec_generic crush pthread)
add_dependencies(ec_shec_generic ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
set_target_properties(ec_shec_generic PROPERTIES VERSION 1.0.0 SOVERSION 1)
install(TARGETS ec_shec_generic DESTINATION lib/erasure-code)

# ec_shec_neon
#TODO:build libec_shec_neon library on an ARM machine
if(ARM_NEON OR ARM_NEON2)
  set(shec_neon_srcs
    $<TARGET_OBJECTS:shec_objs>
    $<TARGET_OBJECTS:jerasure_objs>
    $<TARGET_OBJECTS:neon_objs>
    )
  add_library(ec_shec_neon SHARED ${shec_neon_srcs})
  add_dependencies(ec_shec_neon ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ec_shec_neon ${EXTRALIBS})
  set_target_properties(ec_shec_neon PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${ARM_NEON_FLAGS})
  install(TARGETS ec_shec_neon DESTINATION lib/erasure-code)
endif(ARM_NEON OR ARM_NEON2)

# ec_shec_sse3
if(INTEL_SSE)
  add_library(ec_shec_sse3 SHARED 
    $<TARGET_OBJECTS:jerasure_objs>
    $<TARGET_OBJECTS:shec_objs>)
  add_dependencies(ec_shec_sse3 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ec_shec_sse3 crush ${EXTRALIBS})
  set_target_properties(ec_shec_sse3 PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${SSE3_FLAGS})
  install(TARGETS ec_shec_sse3 DESTINATION lib/erasure-code)
endif(INTEL_SSE)

# ec_shec_sse4
if(INTEL_SSE4_1)
  add_library(ec_shec_sse4 SHARED 
    $<TARGET_OBJECTS:jerasure_objs>
    $<TARGET_OBJECTS:shec_objs>)
  add_dependencies(ec_shec_sse4 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ec_shec_sse4 crush ${EXTRALIBS})
  set_target_properties(ec_shec_sse4 PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${SSE4_FLAGS})
  install(TARGETS ec_shec_sse4 DESTINATION lib/erasure-code)
endif(INTEL_SSE4_1)

add_library(ec_shec SHARED ErasureCodePluginSelectShec.cc)
add_dependencies(ec_shec ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_shec ${EXTRALIBS})
set_target_properties(ec_shec PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ec_shec DESTINATION lib/erasure-code)
