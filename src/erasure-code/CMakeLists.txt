## erasure code plugins

set(erasure_codelibdir ${LIBRARY_OUTPUT_PATH}/erasure-code)

add_subdirectory(jerasure)
add_subdirectory(lrc)
add_subdirectory(shec)

if (HAVE_BETTER_YASM_ELF64)
  add_subdirectory(isa)
endif (HAVE_BETTER_YASM_ELF64)

add_library(erasure_code ErasureCodePlugin.cc)
target_link_libraries(erasure_code dl)
add_dependencies(erasure_code ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)

add_library(erasure_code_objs OBJECT ErasureCode.cc)

add_custom_target(erasure_code_plugins DEPENDS
    ec_isa
    ec_lrc
    ec_jerasure_sse3
    ec_jerasure_sse4
    ec_jerasure)
