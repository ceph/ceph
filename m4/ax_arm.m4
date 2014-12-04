AC_DEFUN([AX_ARM_FEATURES],
[
  AC_REQUIRE([AC_CANONICAL_HOST])

  case $target_cpu in
    arm*)
      AX_CHECK_COMPILE_FLAG(-mfpu=neon, ax_cv_support_neon_ext=yes, [])
      if test x"$ax_cv_support_neon_ext" = x"yes"; then
        ARM_NEON_FLAGS="-mfpu=neon -DARM_NEON"
        AC_SUBST(ARM_NEON_FLAGS)
        ARM_FLAGS="$ARM_FLAGS $ARM_NEON_FLAGS"
        AC_DEFINE(HAVE_NEON,,[Support NEON instructions])
      fi
    ;;
    aarch64*)
      AX_CHECK_COMPILE_FLAG(-march=armv8-a+simd, ax_cv_support_neon_ext=yes, [])
      if test x"$ax_cv_support_neon_ext" = x"yes"; then
        ARM_NEON_FLAGS="-march=armv8-a+simd -DARCH_AARCH64 -DARM_NEON"
        AC_SUBST(ARM_NEON_FLAGS)
        ARM_FLAGS="$ARM_FLAGS $ARM_NEON_FLAGS"
        AC_DEFINE(HAVE_NEON,,[Support NEON instructions])
      fi
    ;;
  esac

  AC_SUBST(ARM_FLAGS)
])
