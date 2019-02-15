## erasure code plugins

set(erasure_plugin_dir ${CEPH_INSTALL_PKGLIBDIR}/erasure-code)

#jerasure subdir must be before shec so jerasure & neon obj libs are declared
include_directories(SYSTEM jerasure/jerasure/include)
include_directories(SYSTEM jerasure/gf-complete/include)
include_directories(jerasure)

# legacy jerasure flavors. these are left here for backward compatibility
# and should be removed in future versions
set(jerasure_legacy_flavors generic)
if(HAVE_ARM)
  list(APPEND jerasure_legacy_flavors neon)
endif()
if(HAVE_INTEL)
 list(APPEND jerasure_legacy_flavors sse3 sse4)
endif()

add_subdirectory(jerasure)
add_subdirectory(lrc)
add_subdirectory(shec)
add_subdirectory(clay)

if (HAVE_BETTER_YASM_ELF64)
  add_subdirectory(isa)
  set(EC_ISA_LIB ec_isa)
endif (HAVE_BETTER_YASM_ELF64)

add_library(erasure_code STATIC ErasureCodePlugin.cc)
target_link_libraries(erasure_code ${CMAKE_DL_LIBS})

add_library(erasure_code_objs OBJECT ErasureCode.cc)

add_custom_target(erasure_code_plugins DEPENDS
    ${EC_ISA_LIB}
    ec_lrc
    ec_jerasure
    ec_shec
    ec_clay)
