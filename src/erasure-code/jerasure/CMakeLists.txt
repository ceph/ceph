# jerasure plugin

include_directories(gf-complete/include)
include_directories(jerasure/include)

set(jerasure_srcs
  jerasure/src/cauchy.c
  jerasure/src/galois.c
  jerasure/src/jerasure.c
  jerasure/src/liberation.c
  jerasure/src/reed_sol.c
  gf-complete/src/gf_wgen.c
  gf-complete/src/gf_method.c
  gf-complete/src/gf_w16.c
  gf-complete/src/gf.c
  gf-complete/src/gf_w32.c
  gf-complete/src/gf_w64.c
  gf-complete/src/gf_w128.c
  gf-complete/src/gf_general.c
  gf-complete/src/gf_w4.c
  gf-complete/src/gf_rand.c
  gf-complete/src/gf_w8.c
  ErasureCodePluginJerasure.cc
  ErasureCodeJerasure.cc
  $<TARGET_OBJECTS:erasure_code_objs>
)

add_library(ec_jerasure_generic SHARED ${jerasure_srcs})
add_dependencies(ec_jerasure_generic ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_jerasure_generic ${EXTRALIBS})
set_target_properties(ec_jerasure_generic PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ec_jerasure_generic DESTINATION lib/erasure-code)

#TODO: LIB_EC_JERASURE_NEON, rough draft of similar target in erasure-code/shec/CMakeLists.txt

# ec_jerasure_sse3
if(INTEL_SSE)
  set(JERASURE_SSE3_FLAGS "-msse")
  if (INTEL_SSE2)
    set(JERASURE_SSE3_FLAGS "${JERASURE_SSE3_FLAGS} -msse2")
  endif (INTEL_SSE2)
  if (INTEL_SSE3)
    set(JERASURE_SSE3_FLAGS "${JERASURE_SSE3_FLAGS} -msse3")
  endif (INTEL_SSE3)
  if (INTEL_SSSE3)
    set(JERASURE_SSE3_FLAGS "${JERASURE_SSE3_FLAGS} -mssse3")
  endif (INTEL_SSSE3)
  add_library(ec_jerasure_sse3 SHARED ${jerasure_srcs})
  add_dependencies(ec_jerasure_sse3 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ec_jerasure_sse3 ${EXTRALIBS})
  set_target_properties(ec_jerasure_sse3 PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${JERASURE_SSE3_FLAGS})
  install(TARGETS ec_jerasure_sse3 DESTINATION lib/erasure-code)
else(INTEL_SSE)
  message(STATUS "Skipping target ec_jerasure_sse3: -msse not supported")
endif(INTEL_SSE)

# ec_jerasure_sse4
if(INTEL_SSE4_1)
  set(JERASURE_SSE4_FLAGS "${JERASURE_SSE3_FLAGS} -msse4.1")
  if (INTEL_SSE4_2)
    set(JERASURE_SSE4_FLAGS "${JERASURE_SSE4_FLAGS} -msse4.2")
  endif (INTEL_SSE4_2)
  add_library(ec_jerasure_sse4 SHARED ${jerasure_srcs})
  add_dependencies(ec_jerasure_sse4 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ec_jerasure_sse4 ${EXTRALIBS})
  set_target_properties(ec_jerasure_sse4 PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${JERASURE_SSE4_FLAGS})
  install(TARGETS ec_jerasure_sse4 DESTINATION lib/erasure-code)
else(INTEL_SSE4_1)
  message(STATUS "Skipping target ec_jerasure_sse4: -msse4.1 not supported")
endif(INTEL_SSE4_1)

add_library(ec_jerasure SHARED ErasureCodePluginSelectJerasure.cc)
add_dependencies(ec_jerasure ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_jerasure ${EXTRALIBS})
set_target_properties(ec_jerasure PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ec_jerasure DESTINATION lib/erasure-code)
