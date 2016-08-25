# jerasure plugin

add_library(jerasure_utils OBJECT
  ErasureCodePluginJerasure.cc
  ErasureCodeJerasure.cc)
add_dependencies(jerasure_utils ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)

add_library(ec_jerasure SHARED ErasureCodePluginSelectJerasure.cc)
add_dependencies(ec_jerasure ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
target_link_libraries(ec_jerasure ${EXTRALIBS})
install(TARGETS ec_jerasure DESTINATION ${erasure_plugin_dir})

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
  gf-complete/src/gf_w8.c)

list(FIND jerasure_flavors generic found)
if(found GREATER -1)
  add_library(jerasure_generic OBJECT ${jerasure_srcs})
endif()

list(FIND jerasure_flavors neon found)
if(found GREATER -1)
  add_library(jerasure_neon OBJECT
    ${jerasure_srcs}
    gf-complete/src/neon/gf_w4_neon.c
    gf-complete/src/neon/gf_w8_neon.c
    gf-complete/src/neon/gf_w16_neon.c
    gf-complete/src/neon/gf_w32_neon.c
    gf-complete/src/neon/gf_w64_neon.c)
  set_target_properties(jerasure_neon PROPERTIES
    COMPILE_FLAGS ${ARM_NEON_FLAGS})
endif()

list(FIND jerasure_flavors sse3 found)
if(found GREATER -1)
  add_library(jerasure_sse3 OBJECT ${jerasure_srcs})
  set_target_properties(jerasure_sse3 PROPERTIES
    COMPILE_FLAGS ${SSE3_FLAGS})
endif()

list(FIND jerasure_flavors sse4 found)
if(found GREATER -1)
  add_library(jerasure_sse4 OBJECT ${jerasure_srcs})
  set_target_properties(jerasure_sse4 PROPERTIES
    COMPILE_FLAGS ${SSE4_FLAGS})
endif()

foreach(flavor ${jerasure_flavors})
  set(plugin_name "ec_jerasure_${flavor}")
  add_library(${plugin_name} SHARED
    $<TARGET_OBJECTS:jerasure_${flavor}>
    $<TARGET_OBJECTS:jerasure_utils>
    $<TARGET_OBJECTS:erasure_code_objs>)
  target_link_libraries(${plugin_name} ${EXTRALIBS})
  install(TARGETS ${plugin_name} DESTINATION ${erasure_plugin_dir})
endforeach()
