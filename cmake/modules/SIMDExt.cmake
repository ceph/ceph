# detect SIMD extentions
#
# ARM_NEON_FLAGS
#
# HAVE_ARMV8_CRC
# HAVE_NEON
# HAVE_SSE
# HAVE_SSE2
#
# INTEL_SSE4_1
# INTEL_SSE4_2
#
# SSE3_FLAGS
# SSE4_FLAGS
#

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64")
  CHECK_C_COMPILER_FLAG(-march=armv8-a+crc HAVE_ARMV8_CRC)
  if(HAVE_ARMV8_CRC)
    set(ARM_NEON_FLAGS "-march=armv8-a+crc -DARCH_AARCH64")
  endif()
  CHECK_C_COMPILER_FLAG(-march=armv8-a+simd HAVE_NEON)
  if(HAVE_NEON)
    set(ARM_NEON_FLAGS "-march=armv8-a+simd -DARCH_AARCH64 -DARM_NEON")
  endif()
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm|ARM")
  CHECK_C_COMPILER_FLAG(-mfpu=neon HAVE_NEON)
  if(HAVE_NEON)
    set(ARM_NEON_FLAGS "-mfpu=neon -DARM_NEON")
  endif()
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "i386|i686|amd64|x86_64|AMD64")
  set(SSE3_FLAGS)
  CHECK_C_COMPILER_FLAG(-msse HAVE_SSE)
  if(HAVE_SSE)
    set(SSE3_FLAGS "${SSE3_FLAGS} -msse -DINTEL_SSE")
  endif()

  CHECK_C_COMPILER_FLAG(-msse2 HAVE_SSE2)
  if(HAVE_SSE2)
    set(SSE3_FLAGS "${SSE3_FLAGS} -msse2 -DINTEL_SSE2")
  endif()

  CHECK_C_COMPILER_FLAG(-msse3 HAVE_SSE3)
  if(HAVE_SSE3)
    set(SSE3_FLAGS "${SSE3_FLAGS} -msse3 -DINTEL_SSE3")
  endif()

  CHECK_C_COMPILER_FLAG(-mssse3 SUPPORTS_SSSE3)
  if(SUPPORTS_SSSE3)
    set(SSE3_FLAGS "${SSE3_FLAGS} -mssse3 -DINTEL_SSSE3")
  endif()

  # pclmul is not enabled in SSE3, otherwise we need another
  # flavor or an cmake option for enabling it

  set(SSE4_FLAGS ${SSE3_FLAGS})
  CHECK_C_COMPILER_FLAG(-msse4.1 INTEL_SSE4_1)
  if(SUPPORTS_SSE41)
    set(SSE4_FLAGS "${SSE4_FLAGS} -msse41 -DINTEL_SSE4")
  endif()

  CHECK_C_COMPILER_FLAG(-msse4.2 INTEL_SSE4_2)
  if(SUPPORTS_SSE42)
    set(SSE4_FLAGS "${SSE4_FLAGS} -msse42 -DINTEL_SSE4")
  endif()
endif()
