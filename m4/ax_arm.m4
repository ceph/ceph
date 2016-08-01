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
      AX_CHECK_COMPILE_FLAG(-march=armv8-a, ax_cv_support_armv8=yes, [])
      if test x"$ax_cv_support_armv8" = x"yes"; then
        ARM_ARCH_FLAGS="-march=armv8-a"
        ARM_DEFINE_FLAGS="-DARCH_AARCH64"
      fi
      AX_CHECK_COMPILE_FLAG(-march=armv8-a+simd, ax_cv_support_neon_ext=yes, [])
      if test x"$ax_cv_support_neon_ext" = x"yes"; then
        ARM_ARCH_FLAGS="$ARM_ARCH_FLAGS+simd"
        ARM_DEFINE_FLAGS="$ARM_DEFINE_FLAGS -DARM_NEON"
        ARM_NEON_FLAGS="-march=armv8-a+simd -DARCH_AARCH64 -DARM_NEON"
        AC_DEFINE(HAVE_NEON,,[Support NEON instructions])
        AC_SUBST(ARM_NEON_FLAGS)
      fi
      AX_CHECK_COMPILE_FLAG(-march=armv8-a+crc, ax_cv_support_crc_ext=yes, [])
      if test x"$ax_cv_support_crc_ext" = x"yes"; then
        ARM_ARCH_FLAGS="$ARM_ARCH_FLAGS+crc"
        ARM_CRC_FLAGS="-march=armv8-a+crc -DARCH_AARCH64"
        AC_DEFINE(HAVE_ARMV8_CRC,,[Support ARMv8 CRC instructions])
        AC_SUBST(ARM_CRC_FLAGS)
      fi
        ARM_FLAGS="$ARM_ARCH_FLAGS $ARM_DEFINE_FLAGS"
    ;;
  esac

  AC_SUBST(ARM_FLAGS)
])
