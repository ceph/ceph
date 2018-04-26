# detect SIMD extentions
#
# HAVE_ARMV8_CRC
# HAVE_ARMV8_SIMD
# HAVE_ARM_NEON
# HAVE_INTEL_SSE
# HAVE_INTEL_SSE2
# HAVE_INTEL_SSE3
# HAVE_INTEL_SSSE3
# HAVE_INTEL_PCLMUL
# HAVE_INTEL_SSE4_1
# HAVE_INTEL_SSE4_2
#
# SIMD_COMPILE_FLAGS
#

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64")
  set(HAVE_ARM 1)
  set(save_quiet ${CMAKE_REQUIRED_QUIET})
  set(CMAKE_REQUIRED_QUIET true)
  include(CheckCXXSourceCompiles)

  check_cxx_source_compiles("
    #define CRC32CX(crc, value) __asm__(\"crc32cx %w[c], %w[c], %x[v]\":[c]\"+r\"(crc):[v]\"r\"(value))
    unsigned int foo(unsigned int ret) {
      CRC32CX(ret, 0);
      return ret;
    }
    int main() { foo(0); }
  " HAVE_ARMV8_CRC)

  check_cxx_source_compiles("
    unsigned int foo(unsigned int ret) {
      __asm__(\"pmull  v2.1q,          v2.1d,  v1.1d\");
      return ret;
    }
    int main() { foo(0); }
  " HAVE_ARMV8_CRYPTO)

  set(CMAKE_REQUIRED_QUIET ${save_quiet})
  if(HAVE_ARMV8_CRC)
    message(STATUS " aarch64 crc extensions supported")
  endif()

  if(HAVE_ARMV8_CRYPTO)
    message(STATUS " aarch64 crypto extensions supported")
  endif()

  check_cxx_source_compiles("
    #include <inttypes.h>
    int main() { uint32_t a; uint8_t b; __builtin_aarch64_crc32b(a, b); }
  " HAVE_ARMV8_CRC_CRYPTO_INTRINSICS)

  if(HAVE_ARMV8_CRC_CRYPTO_INTRINSICS)
    message(STATUS " aarch64 crc+crypto intrinsics supported")
  endif()

  check_cxx_source_compiles("
    unsigned int foo(unsigned int ret) {
      __asm__(\"add v2.1q, v2.1d, v1.1d\");
      return ret;
    }
    int main() { foo(0); }
  " HAVE_ARMV8_SIMD)

elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm|ARM")
  set(HAVE_ARM 1)

  check_cxx_source_compiles("
    unsigned int foo(unsigned int ret) {
      __asm__(\"vadd q2.i32, q2.i32, q1.i32\");
      return ret;
    }
    int main() { foo(0); }
  " HAVE_ARM_NEON)

elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "i386|i686|amd64|x86_64|AMD64")
  set(HAVE_INTEL 1)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "i686|amd64|x86_64|AMD64")
    CHECK_C_COMPILER_FLAG(-msse HAVE_INTEL_SSE)
    if(HAVE_INTEL_SSE)
      set(SIMD_COMPILE_FLAGS "${SIMD_COMPILE_FLAGS} -msse")
    endif()
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64|AMD64")
      CHECK_C_COMPILER_FLAG(-msse2 HAVE_INTEL_SSE2)
      if(HAVE_INTEL_SSE2)
        set(SIMD_COMPILE_FLAGS "${SIMD_COMPILE_FLAGS} -msse2")
      endif()
      CHECK_C_COMPILER_FLAG(-msse3 HAVE_INTEL_SSE3)
      if(HAVE_INTEL_SSE3)
        set(SIMD_COMPILE_FLAGS "${SIMD_COMPILE_FLAGS} -msse3")
      endif()
      CHECK_C_COMPILER_FLAG(-mssse3 HAVE_INTEL_SSSE3)
      if(HAVE_INTEL_SSSE3)
        set(SIMD_COMPILE_FLAGS "${SIMD_COMPILE_FLAGS} -mssse3")
      endif()
      CHECK_C_COMPILER_FLAG(-mpclmul HAVE_INTEL_PCLMUL)
      if(HAVE_INTEL_PCLMUL)
        set(SIMD_COMPILE_FLAGS "${SIMD_COMPILE_FLAGS} -mpclmul")
      endif()
      CHECK_C_COMPILER_FLAG(-msse4.1 HAVE_INTEL_SSE4_1)
      if(HAVE_INTEL_SSE4_1)
        set(SIMD_COMPILE_FLAGS "${SIMD_COMPILE_FLAGS} -msse4.1")
      endif()
      CHECK_C_COMPILER_FLAG(-msse4.2 HAVE_INTEL_SSE4_2)
      if(HAVE_INTEL_SSE4_2)
        set(SIMD_COMPILE_FLAGS "${SIMD_COMPILE_FLAGS} -msse4.2")
      endif()
    endif(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64|AMD64")
  endif(CMAKE_SYSTEM_PROCESSOR MATCHES "i686|amd64|x86_64|AMD64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(powerpc|ppc)64|(powerpc|ppc)64le")
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "(powerpc|ppc)64le")
    set(HAVE_PPC64LE 1)
    message(STATUS " we are ppc64le")
  else()
    set(HAVE_PPC64 1)
    message(STATUS " we are ppc64")
  endif(CMAKE_SYSTEM_PROCESSOR MATCHES "(powerpc|ppc)64le")
  CHECK_C_COMPILER_FLAG("-maltivec" HAS_ALTIVEC)
  if(HAS_ALTIVEC)
    message(STATUS " HAS_ALTIVEC yes")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -maltivec")
    set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -maltivec")
  endif()
  CHECK_C_COMPILER_FLAG("-mcpu=power8" HAVE_POWER8)
  if(HAVE_POWER8)
    message(STATUS " HAVE_POWER8 yes")
  endif()
endif()
