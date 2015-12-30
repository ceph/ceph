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

#TODO:build libec_shec_neon, libec_shec+sse3, libec_shec_sse4 libraries
