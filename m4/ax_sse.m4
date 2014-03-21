AC_DEFUN([AX_SSE],
[
  AC_REQUIRE([AC_CANONICAL_HOST])

  AX_CHECK_COMPILE_FLAG(-msse, ax_cv_support_sse_ext=yes, [])
  if test x"$ax_cv_support_sse_ext" = x"yes"; then
    SIMD_FLAGS="$SIMD_FLAGS -msse -DINTEL_SSE"
    AC_DEFINE(HAVE_SSE,,[Support SSE (Streaming SIMD Extensions) instructions])
  fi

  AX_CHECK_COMPILE_FLAG(-msse2, ax_cv_support_sse2_ext=yes, [])
  if test x"$ax_cv_support_sse2_ext" = x"yes"; then
    SIMD_FLAGS="$SIMD_FLAGS -msse2 -DINTEL_SSE2"
    AC_DEFINE(HAVE_SSE2,,[Support SSE2 (Streaming SIMD Extensions 2) instructions])
  fi

  AX_CHECK_COMPILE_FLAG(-msse3, ax_cv_support_sse3_ext=yes, [])
  if test x"$ax_cv_support_sse3_ext" = x"yes"; then
    SIMD_FLAGS="$SIMD_FLAGS -msse3 -DINTEL_SSE3"
    AC_DEFINE(HAVE_SSE3,,[Support SSE3 (Streaming SIMD Extensions 3) instructions])
  fi

  AX_CHECK_COMPILE_FLAG(-mpclmul, ax_cv_support_pclmuldq_ext=yes, [])
  if test x"$ax_cv_support_pclmuldq_ext" = x"yes"; then
    SIMD_FLAGS="$SIMD_FLAGS -mpclmul -DINTEL_SSE4_PCLMUL"
    AC_DEFINE(HAVE_PCLMULDQ,,[Support (PCLMULDQ) Carry-Free Muliplication])
  fi

  AX_CHECK_COMPILE_FLAG(-mssse3, ax_cv_support_ssse3_ext=yes, [])
  if test x"$ax_cv_support_ssse3_ext" = x"yes"; then
    SIMD_FLAGS="$SIMD_FLAGS -mssse3 -DINTEL_SSSE3"
    AC_DEFINE(HAVE_SSSE3,,[Support SSSE3 (Supplemental Streaming SIMD Extensions 3) instructions])
  fi

  AX_CHECK_COMPILE_FLAG(-msse4.1, ax_cv_support_sse41_ext=yes, [])
  if test x"$ax_cv_support_sse41_ext" = x"yes"; then
    SIMD_FLAGS="$SIMD_FLAGS -msse4.1 -DINTEL_SSE4"
    AC_DEFINE(HAVE_SSE4_1,,[Support SSSE4.1 (Streaming SIMD Extensions 4.1) instructions])
  fi

  AX_CHECK_COMPILE_FLAG(-msse4.2, ax_cv_support_sse42_ext=yes, [])
  if test x"$ax_cv_support_sse42_ext" = x"yes"; then
    SIMD_FLAGS="$SIMD_FLAGS -msse4.2 -DINTEL_SSE4"
    AC_DEFINE(HAVE_SSE4_2,,[Support SSSE4.2 (Streaming SIMD Extensions 4.2) instructions])
  fi

  AX_CHECK_COMPILE_FLAG(-mavx, ax_cv_support_avx_ext=yes, [])
  if test x"$ax_cv_support_avx_ext" = x"yes"; then
    SIMD_FLAGS="$SIMD_FLAGS -mavx"
    AC_DEFINE(HAVE_AVX,,[Support AVX (Advanced Vector Extensions) instructions])
  fi

  AC_SUBST(SIMD_FLAGS)
])
