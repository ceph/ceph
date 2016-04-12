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

if(ARM_NEON OR ARM_NEON2)
  if(ARM_NEON)
    set(ARM_NEON_FLAGS "-mfpu=neon")
  else(ARM_NEON)
    set(ARM_NEON_FLAGS "-march=armv8-a+simd")
  endif(ARM_NEON)
else(ARM_NEON OR ARM_NEON2)
  message(STATUS "Skipping target ec_jerasure_neon & ec_shec_neon: Architecture not ARM")
endif(ARM_NEON OR ARM_NEON2)

if(INTEL_SSE)
  set(SSE3_FLAGS "-msse")
  if (INTEL_SSE2)
    set(SSE3_FLAGS "${SSE3_FLAGS} -msse2")
  endif (INTEL_SSE2)
  if (INTEL_SSE3)
    set(SSE3_FLAGS "${SSE3_FLAGS} -msse3")
  endif (INTEL_SSE3)
  if (INTEL_SSSE3)
    set(SSE3_FLAGS "${SSE3_FLAGS} -mssse3")
  endif (INTEL_SSSE3)
else(INTEL_SSE)
  message(STATUS "Skipping target ec_jerasure_sse3 & ec_shec_sse3: -msse not supported")
endif(INTEL_SSE)

if(INTEL_SSE4_1)
  set(SSE4_FLAGS "${SSE3_FLAGS} -msse4.1")
  if (INTEL_SSE4_2)
    set(SSE4_FLAGS "${SSE4_FLAGS} -msse4.2")
  endif (INTEL_SSE4_2)
else(INTEL_SSE4_1)
  message(STATUS "Skipping target ec_jerasure_sse4 & ec_shec_sse4: -msse4.1 not supported")
endif(INTEL_SSE4_1)

#jerasure subdir must be before shec so jerasure & neon obj libs are declared
include_directories(jerasure/jerasure/include)
include_directories(jerasure/gf-complete/include)
include_directories(jerasure)
add_subdirectory(jerasure)
add_subdirectory(lrc)
add_subdirectory(shec)

if (HAVE_BETTER_YASM_ELF64)
  add_subdirectory(isa)
  set(EC_ISA_LIB ec_isa)
endif (HAVE_BETTER_YASM_ELF64)

add_library(erasure_code ErasureCodePlugin.cc)
target_link_libraries(erasure_code dl)
add_dependencies(erasure_code ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)

add_library(erasure_code_objs OBJECT ErasureCode.cc)

add_custom_target(erasure_code_plugins DEPENDS
    ${EC_ISA_LIB}
    ec_lrc
    ec_jerasure_sse3
    ec_jerasure_sse4
    ec_jerasure)

