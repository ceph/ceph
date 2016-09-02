## erasure code plugins

set(erasure_plugin_dir ${CMAKE_INSTALL_PKGLIBDIR}/erasure-code)

#jerasure subdir must be before shec so jerasure & neon obj libs are declared
include_directories(jerasure/jerasure/include)
include_directories(jerasure/gf-complete/include)
include_directories(jerasure)

include(SIMDExt)

if(TRUE)
  list(APPEND jerasure_flavors generic)
endif()

if(HAVE_NEON)
  list(APPEND jerasure_flavors neon)
endif()

if(HAVE_SSE)
  list(APPEND jerasure_flavors sse3)
endif()

if(INTEL_SSE4_1)
  list(APPEND jerasure_flavors sse4)
endif()

add_subdirectory(jerasure)
add_subdirectory(lrc)
add_subdirectory(shec)

if (HAVE_BETTER_YASM_ELF64)
  add_subdirectory(isa)
  set(EC_ISA_LIB ec_isa)
endif (HAVE_BETTER_YASM_ELF64)

add_library(erasure_code STATIC ErasureCodePlugin.cc)
target_link_libraries(erasure_code ${CMAKE_DL_LIBS})
add_dependencies(erasure_code ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)

add_library(erasure_code_objs OBJECT ErasureCode.cc)

add_custom_target(erasure_code_plugins DEPENDS
    ${EC_ISA_LIB}
    ec_lrc
    ec_jerasure)
if(TARGET ec_jerasure_sse3)
  add_dependencies(erasure_code_plugins ec_jerasure_sse3)
endif()
if(TARGET ec_jerasure_sse4)
  add_dependencies(erasure_code_plugins ec_jerasure_sse4)
endif()
