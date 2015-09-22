## erasure code plugins

set(erasure_codelibdir ${LIBRARY_OUTPUT_PATH}/erasure-code)

## detect sse support

# create a tmp file with an empty main()
set(sse_srcs "${CMAKE_BINARY_DIR}/src/erasure-code/jerasure/tmp_sse.c")
file(WRITE ${sse_srcs} "void main() {}")

# try each -msse flag
try_compile(INTEL_SSE ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-msse")
try_compile(INTEL_SSE2 ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-msse2")
try_compile(INTEL_SSE3 ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-msse3")
try_compile(INTEL_SSSE3 ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-mssse3")
try_compile(INTEL_SSE4_1 ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-msse4.1")
try_compile(INTEL_SSE4_2 ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-msse4.2")
try_compile(ARM_NEON ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-mfpu=neon")
try_compile(ARM_NEON2 ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-march=armv8-a+simd")

# clean up tmp file
file(REMOVE ${sse_srcs})

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

