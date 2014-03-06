# ===========================================================================
#   http://www.gnu.org/software/autoconf-archive/ax_gcc_x86_avx_xgetbv.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_GCC_X86_AVX_XGETBV
#
# DESCRIPTION
#
#   On later x86 processors with AVX SIMD support, with gcc or a compiler
#   that has a compatible syntax for inline assembly instructions, run a
#   small program that executes the xgetbv instruction with input OP. This
#   can be used to detect if the OS supports AVX instruction usage.
#
#   On output, the values of the eax and edx registers are stored as
#   hexadecimal strings as "eax:edx" in the cache variable
#   ax_cv_gcc_x86_avx_xgetbv.
#
#   If the xgetbv instruction fails (because you are running a
#   cross-compiler, or because you are not using gcc, or because you are on
#   a processor that doesn't have this instruction),
#   ax_cv_gcc_x86_avx_xgetbv_OP is set to the string "unknown".
#
#   This macro mainly exists to be used in AX_EXT.
#
# LICENSE
#
#   Copyright (c) 2013 Michael Petch <mpetch@capp-sysware.com>
#
#   This program is free software: you can redistribute it and/or modify it
#   under the terms of the GNU General Public License as published by the
#   Free Software Foundation, either version 3 of the License, or (at your
#   option) any later version.
#
#   This program is distributed in the hope that it will be useful, but
#   WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
#   Public License for more details.
#
#   You should have received a copy of the GNU General Public License along
#   with this program. If not, see <http://www.gnu.org/licenses/>.
#
#   As a special exception, the respective Autoconf Macro's copyright owner
#   gives unlimited permission to copy, distribute and modify the configure
#   scripts that are the output of Autoconf when processing the Macro. You
#   need not follow the terms of the GNU General Public License when using
#   or distributing such scripts, even though portions of the text of the
#   Macro appear in them. The GNU General Public License (GPL) does govern
#   all other use of the material that constitutes the Autoconf Macro.
#
#   This special exception to the GPL applies to versions of the Autoconf
#   Macro released by the Autoconf Archive. When you make and distribute a
#   modified version of the Autoconf Macro, you may extend this special
#   exception to the GPL to apply to your modified version as well.

#serial 1

AC_DEFUN([AX_GCC_X86_AVX_XGETBV],
[AC_REQUIRE([AC_PROG_CC])
AC_LANG_PUSH([C])
AC_CACHE_CHECK(for x86-AVX xgetbv $1 output, ax_cv_gcc_x86_avx_xgetbv_$1,
 [AC_RUN_IFELSE([AC_LANG_PROGRAM([#include <stdio.h>], [
     int op = $1, eax, edx;
     FILE *f;
      /* Opcodes for xgetbv */
      __asm__(".byte 0x0f, 0x01, 0xd0"
        : "=a" (eax), "=d" (edx)
        : "c" (op));
     f = fopen("conftest_xgetbv", "w"); if (!f) return 1;
     fprintf(f, "%x:%x\n", eax, edx);
     fclose(f);
     return 0;
])],
     [ax_cv_gcc_x86_avx_xgetbv_$1=`cat conftest_xgetbv`; rm -f conftest_xgetbv],
     [ax_cv_gcc_x86_avx_xgetbv_$1=unknown; rm -f conftest_xgetbv],
     [ax_cv_gcc_x86_avx_xgetbv_$1=unknown])])
AC_LANG_POP([C])
])
