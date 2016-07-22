# CMake module to get the compile options to match the target 
#
# The parameter COMPILE_TARGETS_FLAGS will be set to the right options 
# according to the HW type
# If ARM NEON instruments set is supported, the following flags are set:
# ARM_NEON
# ARM_NEON2
# If ARM CRC instrument set is supported, the flag HAVE_ARMV8_CRC is set,
# and the related compile options are set
# If INTEL SSE instruments set are supported, the related flags and 
# compile options are set

set(sse_srcs "${CMAKE_BINARY_DIR}/src/test/erasure-code/tmp_sse.c")
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
try_compile(ARM_CRC ${CMAKE_BINARY_DIR} ${sse_srcs}
  COMPILE_DEFINITIONS "-march=armv8-a+crc")

# clean up tmp file
file(REMOVE ${sse_srcs})

if(ARM_CRC)
  set(HAVE_ARMV8_CRC 1)
  set(ARM_CRC_FLAGS "-march=armv8-a+crc -DARCH_AARCH64")
endif(ARM_CRC)

if(ARM_NEON OR ARM_NEON2)
  set(HAVE_NEON 1)
  if(ARM_NEON)
    set(COMPILE_TARGETS_FLAGS "${COMPILE_TARGETS_FLAGS} -mfpu=neon -DARM_NEON")
  else(ARM_NEON)
    set(COMPILE_TARGETS_FLAGS "${COMPILE_TARGETS_FLAGS} -march=armv8-a+simd -DARCH_AARCH64 -DARM_NEON")
  endif(ARM_NEON)
else(ARM_NEON OR ARM_NEON2)
  message(STATUS "Architecture not ARM")
endif(ARM_NEON OR ARM_NEON2)

if(INTEL_SSE)
  set(HAVE_SSE 1)
  set(COMPILE_TARGETS_FLAGS "-msse")
  if (INTEL_SSE2)
    set(HAVE_SSE2 1)
    set(COMPILE_TARGETS_FLAGS "${COMPILE_TARGETS_FLAGS} -msse2")
  endif (INTEL_SSE2)
  if (INTEL_SSE3)
    set(COMPILE_TARGETS_FLAGS "${COMPILE_TARGETS_FLAGS} -msse3")
  endif (INTEL_SSE3)
  if (INTEL_SSSE3)
    set(COMPILE_TARGETS_FLAGS "${COMPILE_TARGETS_FLAGS} -mssse3")
  endif (INTEL_SSSE3)
else(INTEL_SSE)
  message(STATUS "Skipping target ec_jerasure_sse3 & ec_shec_sse3: -msse not supported")
endif(INTEL_SSE)

if(INTEL_SSE4_1)
  set(COMPILE_TARGETS_FLAGS "${COMPILE_TARGETS_FLAGS} -msse4.1")
  if (INTEL_SSE4_2)
    set(COMPILE_TARGETS_FLAGS "${COMPILE_TARGETS_FLAGS} -msse4.2")
  endif (INTEL_SSE4_2)
else(INTEL_SSE4_1)
  message(STATUS "Skipping target ec_jerasure_sse4 & ec_shec_sse4: -msse4.1 not supported")
endif(INTEL_SSE4_1)
