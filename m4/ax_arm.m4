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

      AC_LANG_SAVE
      AC_LANG_CPLUSPLUS
      AC_CACHE_CHECK(whether the assembler supports crc extensions,
                     ax_cv_support_crc_ext, AC_TRY_COMPILE([
          #define CRC32CX(crc, value) __asm__("crc32cx %w[c], %w[c], %x[v]":[c]"+r"(crc):[v]"r"(value))
          asm(".arch_extension crc");
          unsigned int foo(unsigned int ret) {
              CRC32CX(ret, 0);
              return ret;
          }],[ foo(0); ], ax_cv_support_crc_ext=yes, []))
      if test x"$ax_cv_support_crc_ext" = x"yes"; then
        AC_DEFINE(HAVE_ARMV8_CRC,,[Support ARMv8 CRC instructions])
      fi
        ARM_FLAGS="$ARM_ARCH_FLAGS $ARM_DEFINE_FLAGS"
    ;;
  esac

  AC_SUBST(ARM_FLAGS)
])
