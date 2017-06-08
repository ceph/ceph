#shec plugin

set(shec_srcs
  ${CMAKE_SOURCE_DIR}/src/erasure-code/ErasureCode.cc 
  ErasureCodePluginShec.cc 
  ErasureCodeShec.cc 
  ErasureCodeShecTableCache.cc 
  determinant.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/jerasure/src/cauchy.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/jerasure/src/galois.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/jerasure/src/jerasure.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/jerasure/src/liberation.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/jerasure/src/reed_sol.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/gf-complete/src/gf_wgen.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/gf-complete/src/gf_method.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/gf-complete/src/gf_w16.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/gf-complete/src/gf.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/gf-complete/src/gf_w32.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/gf-complete/src/gf_w64.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/gf-complete/src/gf_w128.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/gf-complete/src/gf_general.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/gf-complete/src/gf_w4.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/gf-complete/src/gf_rand.c 
  ${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/gf-complete/src/gf_w8.c)

include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/jerasure/include)
include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure/gf-complete/include)
include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/jerasure)
include_directories(${CMAKE_SOURCE_DIR}/src/erasure-code/shec)
add_library(ec_shec_generic SHARED ${shec_srcs})
target_link_libraries(ec_shec_generic crush pthread)
add_dependencies(ec_shec_generic ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
set_target_properties(ec_shec_generic PROPERTIES VERSION 1.0.0 SOVERSION 1)
install(TARGETS ec_shec_generic DESTINATION lib/erasure-code)

# ec_shec_sse3
set(shec_neon_srcs
  ${shec_srcs}
  erasure-code/jerasure/gf-complete/src/neon/gf_w4_neon.c 
  erasure-code/jerasure/gf-complete/src/neon/gf_w8_neon.c 
  erasure-code/jerasure/gf-complete/src/neon/gf_w16_neon.c 
  erasure-code/jerasure/gf-complete/src/neon/gf_w32_neon.c 
  erasure-code/jerasure/gf-complete/src/neon/gf_w64_neon.c)

#TODO:build libec_shec_neon library on an ARM machine
if(ARM_NEON OR ARM_NEON2)
  add_library(ec_shec_neon SHARED ${shec_neon_srcs})
  add_dependencies(ec_shec_neon ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ec_shec_neon ${EXTRALIBS})
  set_target_properties(ec_shec_neon PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${ARM_NEON_FLAGS})
  install(TARGETS ec_shec_neon DESTINATION lib/erasure-code)
endif(ARM_NEON OR ARM_NEON2)

# ec_shec_sse3
if(INTEL_SSE)
  add_library(ec_shec_sse3 SHARED ${shec_srcs})
  add_dependencies(ec_shec_sse3 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ec_shec_sse3 ${EXTRALIBS})
  set_target_properties(ec_shec_sse3 PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${SSE3_FLAGS})
  install(TARGETS ec_shec_sse3 DESTINATION lib/erasure-code)
endif(INTEL_SSE)

# ec_shec_sse4
if(INTEL_SSE4_1)
  add_library(ec_shec_sse4 SHARED ${shec_srcs})
  add_dependencies(ec_shec_sse4 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ec_shec_sse4 ${EXTRALIBS})
  set_target_properties(ec_shec_sse4 PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${SSE4_FLAGS})
  install(TARGETS ec_shec_sse4 DESTINATION lib/erasure-code)
endif(INTEL_SSE4_1)

add_library(ec_shec SHARED ErasureCodePluginSelectShec.cc)
add_dependencies(ec_shec ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_shec ${EXTRALIBS})
set_target_properties(ec_shec PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ec_shec DESTINATION lib/erasure-code)
