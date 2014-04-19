AC_DEFUN([AX_INTEL_FEATURES],
[
  AC_REQUIRE([AC_CANONICAL_HOST])

  case $target_cpu in 
    i[[3456]]86*|x86_64*|amd64*)
      AX_CHECK_COMPILE_FLAG(-msse, ax_cv_support_sse_ext=yes, [])
      if test x"$ax_cv_support_sse_ext" = x"yes"; then
        INTEL_SSE_FLAGS="-msse -DINTEL_SSE"
        AC_SUBST(INTEL_SSE_FLAGS)
        INTEL_FLAGS="$INTEL_FLAGS $INTEL_SSE_FLAGS"
        AC_DEFINE(HAVE_SSE,,[Support SSE (Streaming SIMD Extensions) instructions])
      fi
    
      AX_CHECK_COMPILE_FLAG(-msse2, ax_cv_support_sse2_ext=yes, [])
      if test x"$ax_cv_support_sse2_ext" = x"yes"; then
        INTEL_SSE2_FLAGS="-msse2 -DINTEL_SSE2"
        AC_SUBST(INTEL_SSE2_FLAGS)
        INTEL_FLAGS="$INTEL_FLAGS $INTEL_SSE2_FLAGS"
        AC_DEFINE(HAVE_SSE2,,[Support SSE2 (Streaming SIMD Extensions 2) instructions])
      fi
    
      AX_CHECK_COMPILE_FLAG(-msse3, ax_cv_support_sse3_ext=yes, [])
      if test x"$ax_cv_support_sse3_ext" = x"yes"; then
        INTEL_SSE3_FLAGS="-msse3 -DINTEL_SSE3"
        AC_SUBST(INTEL_SSE3_FLAGS)
        INTEL_FLAGS="$INTEL_FLAGS $INTEL_SSE3_FLAGS"
        AC_DEFINE(HAVE_SSE3,,[Support SSE3 (Streaming SIMD Extensions 3) instructions])
      fi
    
      AX_CHECK_COMPILE_FLAG(-mssse3, ax_cv_support_ssse3_ext=yes, [])
      if test x"$ax_cv_support_ssse3_ext" = x"yes"; then
        INTEL_SSSE3_FLAGS="-mssse3 -DINTEL_SSSE3"
        AC_SUBST(INTEL_SSSE3_FLAGS)
        INTEL_FLAGS="$INTEL_FLAGS $INTEL_SSSE3_FLAGS"
        AC_DEFINE(HAVE_SSSE3,,[Support SSSE3 (Supplemental Streaming SIMD Extensions 3) instructions])
      fi
    ;;
  esac

  case $target_cpu in
  x86_64*|amd64*)
      AX_CHECK_COMPILE_FLAG(-mpclmul, ax_cv_support_pclmuldq_ext=yes, [])
      if test x"$ax_cv_support_pclmuldq_ext" = x"yes"; then
        INTEL_PCLMUL_FLAGS="-mpclmul -DINTEL_SSE4_PCLMUL"
        AC_SUBST(INTEL_PCLMUL_FLAGS)
        INTEL_FLAGS="$INTEL_FLAGS $INTEL_PCLMUL_FLAGS"
        AC_DEFINE(HAVE_PCLMUL,,[Support (PCLMUL) Carry-Free Muliplication])
      fi
    
      AX_CHECK_COMPILE_FLAG(-msse4.1, ax_cv_support_sse41_ext=yes, [])
      if test x"$ax_cv_support_sse41_ext" = x"yes"; then
        INTEL_SSE4_1_FLAGS="-msse4.1 -DINTEL_SSE4"
        AC_SUBST(INTEL_SSE4_1_FLAGS)
        INTEL_FLAGS="$INTEL_FLAGS $INTEL_SSE4_1_FLAGS"
        AC_DEFINE(HAVE_SSE4_1,,[Support SSE4.1 (Streaming SIMD Extensions 4.1) instructions])
      fi
    
      AX_CHECK_COMPILE_FLAG(-msse4.2, ax_cv_support_sse42_ext=yes, [])
      if test x"$ax_cv_support_sse42_ext" = x"yes"; then
        INTEL_SSE4_2_FLAGS="-msse4.2 -DINTEL_SSE4"
        AC_SUBST(INTEL_SSE4_2_FLAGS)
        INTEL_FLAGS="$INTEL_FLAGS $INTEL_SSE4_2_FLAGS"
        AC_DEFINE(HAVE_SSE4_2,,[Support SSE4.2 (Streaming SIMD Extensions 4.2) instructions])
      fi
    ;;
  esac

  AC_SUBST(INTEL_FLAGS)
])
