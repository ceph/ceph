# jerasure plugin

set(jerasure_utils_src
  ErasureCodePluginJerasure.cc
  ErasureCodeJerasure.cc)

add_library(jerasure_utils OBJECT ${jerasure_utils_src})
target_link_libraries(jerasure_utils legacy-option-headers)

# jerasure_init is ceph's own glue code for galois field initialization,
# not part of the jerasure library itself.
add_library(jerasure_init_objs OBJECT jerasure_init.cc)

if(NOT WITH_SYSTEM_JERASURE)
# Set the CFLAGS correctly for gf-complete based on SIMD compiler support
set(GF_COMPILE_FLAGS)
if(HAVE_ARMV8_SIMD)
  list(APPEND GF_COMPILE_FLAGS ARM_NEON ARCH_AARCH64)
endif()
if(HAVE_ARM_NEON)
  list(APPEND GF_COMPILE_FLAGS ARM_NEON)
endif()
if(HAVE_INTEL_SSE)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE)
endif()
if(HAVE_INTEL_SSE2)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE2)
endif()
if(HAVE_INTEL_SSE3)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE3)
endif()
if(HAVE_INTEL_SSSE3)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSSE3)
endif()
if(HAVE_INTEL_PCLMUL)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE4_PCLMUL)
endif()
if(HAVE_INTEL_SSE4_1)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE4)
endif()
if(HAVE_INTEL_SSE4_2)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE4)
endif()

# set this to TRUE to enable debugging of SIMD detection
# inside gf-complete. gf-complete will printf the SIMD
# instructions detected to stdout.
if (FALSE)
  list(APPEND GF_COMPILE_FLAGS DEBUG_CPU_DETECTION)
endif()

set(gf-complete_srcs
  gf-complete/src/gf_cpu.c
  gf-complete/src/gf_wgen.c
  gf-complete/src/gf_w16.c
  gf-complete/src/gf.c
  gf-complete/src/gf_w32.c
  gf-complete/src/gf_w64.c
  gf-complete/src/gf_w128.c
  gf-complete/src/gf_general.c
  gf-complete/src/gf_w4.c
  gf-complete/src/gf_rand.c
  gf-complete/src/gf_w8.c)

if(HAVE_ARM_NEON OR HAVE_ARMV8_SIMD)
  list(APPEND gf-complete_srcs
    gf-complete/src/neon/gf_w4_neon.c
    gf-complete/src/neon/gf_w8_neon.c
    gf-complete/src/neon/gf_w16_neon.c
    gf-complete/src/neon/gf_w32_neon.c
    gf-complete/src/neon/gf_w64_neon.c)
endif()

add_library(gf-complete_objs OBJECT ${gf-complete_srcs})
set_target_properties(gf-complete_objs PROPERTIES
  COMPILE_FLAGS "${SIMD_COMPILE_FLAGS}")
set_target_properties(gf-complete_objs PROPERTIES
  COMPILE_DEFINITIONS "${GF_COMPILE_FLAGS}")

set(jerasure_srcs
  jerasure/src/cauchy.c
  jerasure/src/galois.c
  jerasure/src/jerasure.c
  jerasure/src/liberation.c
  jerasure/src/reed_sol.c)
add_library(jerasure_lib_objs OBJECT ${jerasure_srcs})
target_compile_options(jerasure_lib_objs PRIVATE "-Wno-unused-but-set-variable")

# Provide the vendored jerasure + gf-complete as a single static library
# target, using the same Jerasure::jerasure name that FindJerasure.cmake
# provides for the system library case.
add_library(jerasure_vendored STATIC
  $<TARGET_OBJECTS:gf-complete_objs>
  $<TARGET_OBJECTS:jerasure_lib_objs>)
add_library(Jerasure::jerasure ALIAS jerasure_vendored)
endif()

# ec_jerasure plugin
set(ec_jerasure_objs
  $<TARGET_OBJECTS:jerasure_init_objs>
  $<TARGET_OBJECTS:jerasure_utils>
  $<TARGET_OBJECTS:erasure_code_objs>)

add_library(ec_jerasure SHARED ${ec_jerasure_objs})
set_target_properties(ec_jerasure PROPERTIES
  INSTALL_RPATH "")
target_link_libraries(ec_jerasure Jerasure::jerasure ${EXTRALIBS})
install(TARGETS ec_jerasure DESTINATION ${erasure_plugin_dir})

# legacy libraries
foreach(flavor ${jerasure_legacy_flavors})
  set(plugin_name "ec_jerasure_${flavor}")
  add_library(${plugin_name} SHARED ${ec_jerasure_objs})
  set_target_properties(${plugin_name} PROPERTIES
    INSTALL_RPATH "")
  target_link_libraries(${plugin_name} Jerasure::jerasure)
  install(TARGETS ${plugin_name} DESTINATION ${erasure_plugin_dir})
  add_dependencies(ec_jerasure ${plugin_name})
endforeach()
