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

add_library(ceph_jerasure_objs OBJECT
  ErasureCodePluginJerasure.cc
  ErasureCodeJerasure.cc
  )

add_library(ceph_jerasure_generic SHARED 
  $<TARGET_OBJECTS:ceph_jerasure_objs>
  $<TARGET_OBJECTS:jerasure_objs>
  $<TARGET_OBJECTS:erasure_code_objs>
  )
add_dependencies(ceph_jerasure_generic ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_jerasure_generic ${EXTRALIBS})
set_target_properties(ceph_jerasure_generic PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ceph_jerasure_generic DESTINATION ${erasure_plugin_dir})

# ceph_jerasure_neon
#TODO:build libec_jerasure_neon library on an ARM machine
if(ARM_NEON OR ARM_NEON2)
  set(neon_objs_srcs
    gf-complete/src/neon/gf_w4_neon.c
    gf-complete/src/neon/gf_w8_neon.c
    gf-complete/src/neon/gf_w16_neon.c
    gf-complete/src/neon/gf_w32_neon.c
    gf-complete/src/neon/gf_w64_neon.c)
  add_library(neon_objs OBJECT ${neon_objs_srcs})
  set_target_properties(neon_objs PROPERTIES COMPILE_FLAGS ${ARM_NEON_FLAGS})

  set(jerasure_neon_srcs
    $<TARGET_OBJECTS:ceph_jerasure_objs>
    $<TARGET_OBJECTS:jerasure_objs>
    $<TARGET_OBJECTS:neon_objs>
    $<TARGET_OBJECTS:erasure_code_objs>
    )
  add_library(ceph_jerasure_neon SHARED ${jerasure_neon_srcs})
  add_dependencies(ceph_jerasure_neon ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ceph_jerasure_neon crush pthread ${EXTRALIBS})
  set_target_properties(ceph_jerasure_neon PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${ARM_NEON_FLAGS})
  install(TARGETS ceph_jerasure_neon DESTINATION ${erasure_plugin_dir})
endif(ARM_NEON OR ARM_NEON2)

# ceph_jerasure_sse3
if(INTEL_SSE)
  add_library(ceph_jerasure_sse3 SHARED 
    $<TARGET_OBJECTS:jerasure_objs>
    $<TARGET_OBJECTS:ceph_jerasure_objs>
    $<TARGET_OBJECTS:erasure_code_objs>
    )
  add_dependencies(ceph_jerasure_sse3 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ceph_jerasure_sse3 crush ${EXTRALIBS})
  set_target_properties(ceph_jerasure_sse3 PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${SSE3_FLAGS})
  install(TARGETS ceph_jerasure_sse3 DESTINATION ${erasure_plugin_dir})
endif(INTEL_SSE)

# ceph_jerasure_sse4
if(INTEL_SSE4_1)
  add_library(ceph_jerasure_sse4 SHARED 
    $<TARGET_OBJECTS:jerasure_objs>
    $<TARGET_OBJECTS:ceph_jerasure_objs>
    $<TARGET_OBJECTS:erasure_code_objs>
    )
  add_dependencies(ceph_jerasure_sse4 ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
  target_link_libraries(ceph_jerasure_sse4 crush ${EXTRALIBS})
  set_target_properties(ceph_jerasure_sse4 PROPERTIES VERSION 2.0.0 SOVERSION 2
    COMPILE_FLAGS ${SSE4_FLAGS})
  install(TARGETS ceph_jerasure_sse4 DESTINATION ${erasure_plugin_dir})
endif(INTEL_SSE4_1)

add_library(ceph_jerasure SHARED ErasureCodePluginSelectJerasure.cc)
add_dependencies(ceph_jerasure ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ceph_jerasure ${EXTRALIBS})
set_target_properties(ceph_jerasure PROPERTIES VERSION 2.0.0 SOVERSION 2)
install(TARGETS ceph_jerasure DESTINATION ${erasure_plugin_dir})
