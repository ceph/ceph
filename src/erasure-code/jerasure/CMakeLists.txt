# jerasure plugin

set(jerasure_obj_srcs
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
  )
add_library(jerasure_objs OBJECT ${jerasure_obj_srcs})

add_library(ec_jerasure_objs OBJECT
  ErasureCodePluginJerasure.cc
  ErasureCodeJerasure.cc
  )

add_library(ec_jerasure_generic SHARED 
  $<TARGET_OBJECTS:ec_jerasure_objs>
  $<TARGET_OBJECTS:jerasure_objs>
  $<TARGET_OBJECTS:erasure_code_objs>
  )
add_dependencies(ec_jerasure_generic ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_jerasure_generic ${EXTRALIBS})
set_target_properties(ec_jerasure_generic PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ec_jerasure_generic DESTINATION lib/erasure-code)

# ec_jerasure_neon
#TODO:build libec_jerasure_neon library on an ARM machine
if(ARM_NEON OR ARM_NEON2)
  set(neon_objs_srcs
    gf-complete/src/neon/gf_w4_neon.c
    gf-complete/src/neon/gf_w8_neon.c
    gf-complete/src/neon/gf_w16_neon.c
    gf-complete/src/neon/gf_w32_neon.c
    gf-complete/src/neon/gf_w64_neon.c)
  add_library(neon_objs OBJECT ${neon_objs_srcs})

  set(jerasure_neon_srcs
    $<TARGET_OBJECTS:ec_jerasure_objs>
    $<TARGET_OBJECTS:jerasure_objs>
    $<TARGET_OBJECTS:neon_objs>
    $<TARGET_OBJECTS:erasure_code_objs>
    )
  add_library(ec_jerasure_neon SHARED ${jerasure_neon_srcs})
  add_dependencies(ec_jerasure_neon ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ec_jerasure_neon crush pthread ${EXTRALIBS})
  set_target_properties(ec_jerasure_neon PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${ARM_NEON_FLAGS})
  install(TARGETS ec_jerasure_neon DESTINATION lib/erasure-code)
endif(ARM_NEON OR ARM_NEON2)

# ec_jerasure_sse3
if(INTEL_SSE)
  add_library(ec_jerasure_sse3 SHARED 
    $<TARGET_OBJECTS:jerasure_objs>
    $<TARGET_OBJECTS:ec_jerasure_objs>
    $<TARGET_OBJECTS:erasure_code_objs>
    )
  add_dependencies(ec_jerasure_sse3 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ec_jerasure_sse3 ${EXTRALIBS})
  set_target_properties(ec_jerasure_sse3 PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${SSE3_FLAGS})
  install(TARGETS ec_jerasure_sse3 DESTINATION lib/erasure-code)
endif(INTEL_SSE)

# ec_jerasure_sse4
if(INTEL_SSE4_1)
  add_library(ec_jerasure_sse4 SHARED 
    $<TARGET_OBJECTS:jerasure_objs>
    $<TARGET_OBJECTS:ec_jerasure_objs>
    $<TARGET_OBJECTS:erasure_code_objs>
    )
  add_dependencies(ec_jerasure_sse4 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ec_jerasure_sse4 ${EXTRALIBS})
  set_target_properties(ec_jerasure_sse4 PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${SSE4_FLAGS})
  install(TARGETS ec_jerasure_sse4 DESTINATION lib/erasure-code)
endif(INTEL_SSE4_1)

add_library(ec_jerasure SHARED ErasureCodePluginSelectJerasure.cc)
add_dependencies(ec_jerasure ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_jerasure ${EXTRALIBS})
set_target_properties(ec_jerasure PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ec_jerasure DESTINATION lib/erasure-code)
